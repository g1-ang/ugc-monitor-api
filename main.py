"""
main.py — UGC Monitor FastAPI 백엔드
────────────────────────────────────
Render에 배포되는 백엔드 서버입니다.
대시보드(Vercel)에서 호출하면 Phase 1~3을 순서대로 실행합니다.

엔드포인트:
  POST /scan    → URL + 레퍼런스 이미지로 전체 스캔 시작
  GET  /results → 최신 스캔 결과 조회
  GET  /health  → 서버 상태 확인
"""

import os, io, time, base64, requests
from datetime import datetime, timezone

from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from PIL import Image

import gspread
from google.oauth2.service_account import Credentials

load_dotenv()

# ── 설정 ───────────────────────────────────────
APIFY_API_TOKEN    = os.getenv("APIFY_API_TOKEN")
SPREADSHEET_ID     = os.getenv("SPREADSHEET_ID")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS_PATH", "config/google_credentials.json")
NAVER_API_URL      = os.getenv("NAVER_API_URL", "").rstrip("/")
NAVER_API_KEY      = os.getenv("NAVER_API_KEY")
MY_IG_ID           = "pitapat_prompt"
SHEET_TAB_NAME     = "ugc_users"

APIFY_BASE     = "https://api.apify.com/v2"
ACTOR_COMMENTS = "apify~instagram-comment-scraper"
ACTOR_PROFILE  = "apify~instagram-profile-scraper"
MODEL_NAME     = "Qwen2.5-VL-32B-Instruct"

PROMPT = """아래 두 이미지를 비교해주세요.

[이미지 1]은 레퍼런스 스타일 샘플입니다.
[이미지 2]는 판별 대상입니다.

판별 기준:
- 두 이미지가 비슷한 AI 생성 스타일인가?
- 비슷한 인물 표현 방식(얼굴 비율, 피부 보정, 분위기)인가?
- 같은 AI 프롬프트나 도구로 만들었을 가능성이 있는가?

반드시 YES 또는 NO 한 단어만 답하세요."""

app = FastAPI(title="UGC Monitor API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

scan_state = {
    "status":     "idle",   # idle / running / done / error
    "progress":   0,
    "step":       "",
    "results":    [],
    "stats":      {"feed": 0, "story": 0, "profile": 0},
    "started_at": None,
}


# ── 이미지 리사이즈 ────────────────────────────
def resize_to_data_uri(raw: bytes, max_side: int = 1024) -> str:
    """바이트 → 리사이즈된 data URI"""
    try:
        img = Image.open(io.BytesIO(raw))
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        img.thumbnail((max_side, max_side), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
        return f"data:image/jpeg;base64,{b64}"
    except Exception:
        b64 = base64.b64encode(raw).decode("utf-8")
        return f"data:image/jpeg;base64,{b64}"


# ── Google Sheets ─────────────────────────────
def get_sheet():
    creds = Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS,
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds).open_by_key(SPREADSHEET_ID).worksheet(SHEET_TAB_NAME)


def sheet_range(cell: str) -> str:
    """시트명 접두사 붙인 A1 range"""
    return f"'{SHEET_TAB_NAME}'!{cell}"


# ── Apify 실행 헬퍼 ────────────────────────────
def run_apify(actor_id: str, run_input: dict, timeout: int = 200) -> list:
    resp = requests.post(
        f"{APIFY_BASE}/acts/{actor_id}/runs?token={APIFY_API_TOKEN}",
        json=run_input, timeout=30,
    )
    resp.raise_for_status()
    run_id = resp.json()["data"]["id"]

    deadline = time.time() + timeout
    status_data = {}
    while time.time() < deadline:
        time.sleep(6)
        r = requests.get(f"{APIFY_BASE}/actor-runs/{run_id}?token={APIFY_API_TOKEN}", timeout=15)
        status_data = r.json()["data"]
        if status_data["status"] == "SUCCEEDED":
            break
        if status_data["status"] in ("FAILED", "ABORTED", "TIMED-OUT"):
            return []

    dataset_id = status_data.get("defaultDatasetId", "")
    if not dataset_id:
        return []

    return requests.get(
        f"{APIFY_BASE}/datasets/{dataset_id}/items?token={APIFY_API_TOKEN}",
        timeout=30,
    ).json()


# ── NAVER Open Models (Qwen2.5-VL) 판별 ───────
def call_qwen(reference_data_uri: str, target_url: str, max_retries: int = 3) -> bool | None:
    """레퍼런스(data URI) vs 타겟(URL) 비교"""
    payload = {
        "model": MODEL_NAME,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": PROMPT},
                {"type": "text", "text": "[이미지 1] 레퍼런스:"},
                {"type": "image_url", "image_url": {"url": reference_data_uri}},
                {"type": "text", "text": "[이미지 2] 판별 대상:"},
                {"type": "image_url", "image_url": {"url": target_url}},
            ],
        }],
        "temperature": 0.1,
        "max_tokens": 10,
    }
    headers = {
        "Authorization": f"Bearer {NAVER_API_KEY}",
        "Content-Type":  "application/json",
    }
    endpoint = f"{NAVER_API_URL}/chat/completions"

    for attempt in range(max_retries):
        try:
            resp = requests.post(endpoint, json=payload, headers=headers, timeout=90)
            if resp.status_code in (429, 503):
                time.sleep(2 ** (attempt + 2))
                continue
            resp.raise_for_status()
            answer = resp.json()["choices"][0]["message"]["content"].strip().upper()
            return "YES" in answer
        except Exception as e:
            print(f"⚠️ Qwen 호출 실패: {e}")
            return None
    return None


# ── 전체 스캔 파이프라인 ───────────────────────
def run_full_scan(post_url: str, reference_data_uri: str):
    global scan_state
    scan_state.update({
        "status": "running", "progress": 5, "step": "댓글 수집 중...",
        "results": [], "started_at": datetime.now().isoformat(),
    })

    try:
        sheet = get_sheet()

        # ── Phase 1: 댓글 수집 ────────────────
        scan_state.update({"progress": 10, "step": "댓글 유저 수집 중..."})
        comments = run_apify(ACTOR_COMMENTS, {
            "directUrls": [post_url], "resultsLimit": 500,
            "_triggeredBy": "지원", "_project": "프롬프트 오가닉 모니터링",
        })

        user_map = {}
        for c in comments:
            u = c.get("ownerUsername") or c.get("username") or (c.get("owner") or {}).get("username")
            if u and u.lower() != MY_IG_ID.lower():
                user_map[u] = c.get("postUrl") or post_url

        existing = {r[0].strip() for r in sheet.get_all_values()[1:] if r and r[0]}
        to_add   = {u: v for u, v in user_map.items() if u not in existing}
        if to_add:
            now  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            rows = [[u,"",now,"","","","","none","",v,""] for u, v in sorted(to_add.items())]
            sheet.append_rows(rows, value_input_option="RAW")

        scan_state.update({"progress": 30, "step": f"프로필 스캔 중... ({len(user_map)}명)"})

        # ── Phase 2: 프로필 스캔 ──────────────
        usernames = list(user_map.keys())
        profiles  = []
        chunks    = [usernames[i:i+50] for i in range(0, len(usernames), 50)]

        for idx, chunk in enumerate(chunks):
            p = run_apify(ACTOR_PROFILE, {
                "usernames": chunk, "resultsLimit": 1,
                "_triggeredBy": "지원", "_project": "프롬프트 오가닉 모니터링",
            })
            profiles.extend(p)
            progress = 30 + int((idx+1) / len(chunks) * 30)
            scan_state.update({
                "progress": progress,
                "step": f"프로필 스캔 중... ({(idx+1)*50}/{len(usernames)}명)",
            })

        profile_map = {}
        for p in profiles:
            uname = p.get("username", "")
            if uname:
                profile_map[uname.lower()] = p

        # 판별 후보 구성
        candidates = []
        for uname, p in profile_map.items():
            has_story    = p.get("hasPublicStory", False)
            stories      = p.get("stories") or p.get("latestStories") or []
            story_image  = ""
            if stories and isinstance(stories, list):
                s0 = stories[0]
                story_image = s0.get("displayUrl") or s0.get("imageUrl") or s0.get("url", "")

            latest_posts = p.get("latestPosts") or p.get("posts") or []
            feed_items   = []
            for lp in latest_posts[:5]:
                img_url = lp.get("displayUrl") or lp.get("imageUrl") or ""
                sc      = lp.get("shortCode") or lp.get("shortcode") or ""
                p_url   = f"https://www.instagram.com/p/{sc}/" if sc else lp.get("url", "")
                if img_url:
                    feed_items.append({"image_url": img_url, "post_url": p_url})

            profile_url = p.get("profilePicUrl") or p.get("profilePicUrlHD", "")

            if has_story or feed_items or profile_url:
                candidates.append({
                    "username":    uname,
                    "has_story":   has_story,
                    "story_image": story_image,
                    "feed_items":  feed_items,
                    "profile_url": profile_url,
                })

        scan_state.update({"progress": 65, "step": f"AI 이미지 판별 중... (0/{len(candidates)}명)"})

        # ── Phase 3: Qwen 판별 ────────────────
        confirmed = []
        for i, user in enumerate(candidates):
            uname = user["username"]

            # 판별 순서: 프사 → 스토리 → 피드 5개
            images_to_check = []
            if user.get("profile_url"):
                images_to_check.append(("profile", user["profile_url"], ""))
            if user.get("has_story") and user.get("story_image"):
                images_to_check.append(("story", user["story_image"], ""))
            for item in user.get("feed_items", []):
                images_to_check.append(("feed", item["image_url"], item.get("post_url", "")))

            matched_type = None
            matched_link = None

            for img_type, img_url, p_url in images_to_check:
                result = call_qwen(reference_data_uri, img_url)
                if result is True:
                    matched_type = img_type
                    matched_link = p_url or None
                    break
                time.sleep(1)

            if matched_type:
                confirmed.append({
                    "username":    uname,
                    "detected_at": datetime.now().strftime("%H:%M"),
                    "type":        matched_type,
                    "link":        matched_link,
                })

                # Sheets 업데이트 (ugc_users! 접두사 포함)
                all_vals = sheet.get_all_values()
                for row_i, row in enumerate(all_vals[1:], start=2):
                    if row and row[0].strip().lower() == uname.lower():
                        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                        sheet.spreadsheet.values_batch_update({
                            "valueInputOption": "RAW",
                            "data": [
                                {"range": sheet_range(f"G{row_i}"), "values": [["TRUE"]]},
                                {"range": sheet_range(f"H{row_i}"), "values": [[matched_type]]},
                                {"range": sheet_range(f"I{row_i}"), "values": [[now]]},
                            ],
                        })
                        break

            progress = 65 + int((i+1) / max(len(candidates), 1) * 30)
            scan_state.update({
                "progress": progress,
                "step": f"AI 이미지 판별 중... ({i+1}/{len(candidates)}명)",
            })

        # ── 완료 ──────────────────────────────
        feed_n    = len([r for r in confirmed if r["type"] == "feed"])
        story_n   = len([r for r in confirmed if r["type"] == "story"])
        profile_n = len([r for r in confirmed if r["type"] == "profile"])

        scan_state.update({
            "status":   "done",
            "progress": 100,
            "step":     "완료!",
            "results":  confirmed,
            "stats":    {"feed": feed_n, "story": story_n, "profile": profile_n},
        })

    except Exception as e:
        scan_state.update({"status": "error", "step": f"오류: {str(e)}"})


# ── API 엔드포인트 ─────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now().isoformat()}


@app.post("/scan")
async def start_scan(
    background_tasks: BackgroundTasks,
    post_url: str = Form(...),
    reference_image: UploadFile = File(...),
):
    if scan_state["status"] == "running":
        return JSONResponse({"error": "이미 스캔이 진행 중입니다."}, status_code=409)

    img_bytes = await reference_image.read()
    ref_uri   = resize_to_data_uri(img_bytes)

    background_tasks.add_task(run_full_scan, post_url, ref_uri)
    return {"status": "started", "post_url": post_url}


@app.get("/results")
def get_results():
    return scan_state


@app.get("/")
def root():
    return {"message": "UGC Monitor API", "docs": "/docs"}
