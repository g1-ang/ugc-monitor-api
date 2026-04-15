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

import os, io, time, base64, csv, requests
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from PIL import Image
from openpyxl import load_workbook

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
ACTOR_PROFILE  = "apify~instagram-profile-scraper"
ACTOR_STORY    = "seemuapps~instagram-story-scraper"
USERNAME_HEADERS = {"username", "user", "userid", "user_id", "아이디", "id"}
MODEL_NAME     = "Qwen2.5-VL-32B-Instruct"

PROMPT_FEED = """[이미지 1]은 특정 AI 프롬프트로 만든 레퍼런스 결과물입니다.
[이미지 2]는 유저가 올린 판별 대상 피드 게시물입니다.

배경: 이 프롬프트는 여러 유저가 자기 얼굴로 동일하게 생성하는 구조입니다.
동일 프롬프트로 만든 이미지는 **얼굴만 다르고 장면·구도·무드가 거의 동일**합니다.

얼굴(인물 identity)은 무시하고, 아래 6가지 요소 중 [이미지 1]과 [이미지 2]에서 얼마나 유사한지 판단:
1. 배경·장소 (같은 씬/공간 유형)
2. 의상·소품 (같은 착장이나 핵심 소품)
3. 카메라 구도/앵글 (셀피·하이앵글·거울샷 등)
4. 조명·노출 (광원 방향, 밝기, 분위기)
5. 색감·톤 (팔레트, 화이트밸런스)
6. 전체적 무드/스타일

판별 규칙:
- 얼굴이 달라도 상관없음
- 위 6가지 중 **핵심 3가지 이상**이 명확히 유사하면 YES
- 단순히 "AI 이미지"거나 "여성 셀카"라는 이유만으로는 NO
- 배경과 구도 둘 다 완전히 다르면 NO

반드시 YES 또는 NO 한 단어만 답하세요."""

PROMPT_PROFILE = """[이미지 1]은 특정 AI 프롬프트로 만든 레퍼런스 결과물입니다.
[이미지 2]는 유저의 프로필 사진입니다 (보통 150×150 저해상도).

배경: 이 프롬프트는 여러 유저가 자기 얼굴로 동일하게 생성하는 구조입니다.
프로필 사진은 작고 크롭되어 있을 수 있지만, 핵심 구도/배경이 같으면 같은 프롬프트로 판단합니다.

얼굴은 완전히 무시하고, 아래 요소가 얼마나 비슷한지만 봅니다:
1. 배경·장소 (같은 씬/공간 유형)
2. 의상 또는 소품 (있다면)
3. 카메라 구도·앵글 (거울샷, 셀피, 하이앵글 등)
4. 전체 분위기·무드

판별 규칙:
- 저해상도·크롭이어도 핵심 구도·배경이 유사하면 YES
- 위 4가지 중 **2가지 이상**이 명확히 유사하면 YES
- "둘 다 AI 이미지" 또는 "둘 다 셀카"라는 표면적 공통점만으론 NO
- 배경과 구도 둘 다 완전히 다르면 NO

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


def video_url_to_data_uri(video_url: str) -> str | None:
    """비디오 URL → 첫 프레임 → data URI. imageio-ffmpeg 번들 바이너리 사용"""
    try:
        import subprocess
        import imageio_ffmpeg
        ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
        # 첫 프레임만 JPEG 파이프로 추출
        proc = subprocess.run(
            [ffmpeg, "-hide_banner", "-loglevel", "error",
             "-i", video_url, "-frames:v", "1", "-f", "image2pipe",
             "-vcodec", "mjpeg", "-"],
            capture_output=True, timeout=30,
        )
        if proc.returncode != 0 or not proc.stdout:
            return None
        img = Image.open(io.BytesIO(proc.stdout))
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        img.thumbnail((1024, 1024), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        return "data:image/jpeg;base64," + base64.b64encode(buf.getvalue()).decode("utf-8")
    except Exception:
        return None


def target_to_qwen_url(url: str) -> str | None:
    """이미지면 그대로, 비디오면 첫 프레임 data URI"""
    if not url:
        return None
    lower = url.lower().split("?")[0]
    if lower.endswith((".mp4", ".mov", ".webm")):
        return video_url_to_data_uri(url)
    return url


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
def call_qwen(reference_data_uri: str, target_url: str, img_type: str = "feed", max_retries: int = 3) -> bool | None:
    """레퍼런스(data URI) vs 타겟(URL) 비교. img_type='profile'이면 저해상도 프롬프트"""
    prompt = PROMPT_PROFILE if img_type in ("profile", "story") else PROMPT_FEED
    target = target_to_qwen_url(target_url)
    if not target:
        return None
    payload = {
        "model": MODEL_NAME,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {"type": "text", "text": "[이미지 1] 레퍼런스:"},
                {"type": "image_url", "image_url": {"url": reference_data_uri}},
                {"type": "text", "text": "[이미지 2] 판별 대상:"},
                {"type": "image_url", "image_url": {"url": target}},
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
def parse_comment_file(content: bytes, filename: str) -> list[str]:
    """xlsx 또는 csv에서 username 리스트 추출"""
    ext = os.path.splitext(filename)[1].lower()
    if ext == ".xlsx":
        wb = load_workbook(io.BytesIO(content), read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return []
        header = [str(c).strip().lower() if c else "" for c in rows[0]]
        col_idx = next((i for i, h in enumerate(header) if h in USERNAME_HEADERS), 0)
        return [str(r[col_idx]).strip() for r in rows[1:] if r and r[col_idx]]
    elif ext == ".csv":
        text = content.decode("utf-8-sig")
        reader = list(csv.reader(io.StringIO(text)))
        if not reader:
            return []
        header = [c.strip().lower() for c in reader[0]]
        col_idx = next((i for i, h in enumerate(header) if h in USERNAME_HEADERS), 0)
        return [r[col_idx].strip() for r in reader[1:] if r and r[col_idx]]
    else:
        raise ValueError(f"지원하지 않는 파일 형식: {ext}")


def run_full_scan(comment_file_bytes: bytes, comment_filename: str,
                  reference_data_uris: list, post_url: str = ""):
    global scan_state
    scan_state.update({
        "status": "running", "progress": 5, "step": "댓글 파일 파싱 중...",
        "results": [], "started_at": datetime.now().isoformat(),
    })

    try:
        sheet = get_sheet()

        # ── Phase 1: 댓글 파일 파싱 & 시트 추가 ──
        raw_usernames = parse_comment_file(comment_file_bytes, comment_filename)
        # 본인 제외 + 중복 제거
        seen = set()
        usernames = []
        for u in raw_usernames:
            if not u or u.lower() == MY_IG_ID.lower() or u.lower() in seen:
                continue
            seen.add(u.lower())
            usernames.append(u)

        scan_state.update({"progress": 15, "step": f"댓글 유저 {len(usernames)}명 확인"})

        existing = {r[0].strip() for r in sheet.get_all_values()[1:] if r and r[0]}
        to_add   = [u for u in usernames if u not in existing]
        if to_add:
            now  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            rows = [[u,"",now,"","","","","none","",post_url,""] for u in to_add]
            sheet.append_rows(rows, value_input_option="RAW")

        scan_state.update({"progress": 25, "step": f"프로필 스캔 중... ({len(usernames)}명)"})

        # ── Phase 2: 프로필 스캔 ──────────────
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

        # ── Phase 2b: 스토리 스캔 (별도 actor) ──
        scan_state.update({"progress": 62, "step": "스토리 스캔 중..."})
        story_map = {}  # username → list[url]
        story_chunks = [usernames[i:i+20] for i in range(0, len(usernames), 20)]
        for idx, chunk in enumerate(story_chunks):
            try:
                items = run_apify(ACTOR_STORY, {"usernames": chunk}, timeout=180)
                for it in items:
                    u = (it.get("username") or "").lower()
                    stories = it.get("stories") or []
                    urls = [s.get("mediaUrl") for s in stories if s.get("mediaUrl")]
                    if u and urls:
                        story_map[u] = urls
            except Exception as e:
                print(f"story batch {idx+1} 실패: {e}")
            if idx < len(story_chunks) - 1:
                time.sleep(2)

        # 판별 후보 구성
        candidates = []
        for uname, p in profile_map.items():
            story_urls = story_map.get(uname.lower(), [])
            has_story  = bool(story_urls) or p.get("hasPublicStory", False)

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
                    "story_urls":  story_urls,
                    "feed_items":  feed_items,
                    "profile_url": profile_url,
                })

        scan_state.update({"progress": 65, "step": f"AI 이미지 판별 중... (0/{len(candidates)}명)"})

        # ── Phase 3: Qwen 판별 ────────────────
        confirmed = []
        for i, user in enumerate(candidates):
            uname = user["username"]

            # 판별 순서: 프사 → 스토리(여러 장) → 피드 5개
            images_to_check = []
            if user.get("profile_url"):
                images_to_check.append(("profile", user["profile_url"], ""))
            for s_url in user.get("story_urls", []):
                images_to_check.append(("story", s_url, ""))
            for item in user.get("feed_items", []):
                images_to_check.append(("feed", item["image_url"], item.get("post_url", "")))

            matched_type = None
            matched_link = None

            for img_type, img_url, p_url in images_to_check:
                for ref_uri in reference_data_uris:
                    result = call_qwen(ref_uri, img_url, img_type)
                    if result is True:
                        matched_type = img_type
                        matched_link = p_url or None
                        break
                if matched_type:
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
    comment_file: UploadFile = File(...),
    post_url: str = Form(""),
    reference_image_1: Optional[UploadFile] = File(None),
    reference_image_2: Optional[UploadFile] = File(None),
    reference_image_3: Optional[UploadFile] = File(None),
    reference_image_4: Optional[UploadFile] = File(None),
    reference_image_5: Optional[UploadFile] = File(None),
):
    if scan_state["status"] == "running":
        return JSONResponse({"error": "이미 스캔이 진행 중입니다."}, status_code=409)

    ref_files = [f for f in [reference_image_1, reference_image_2, reference_image_3,
                              reference_image_4, reference_image_5] if f is not None]
    if not ref_files:
        return JSONResponse({"error": "레퍼런스 이미지를 1장 이상 업로드해주세요."}, status_code=422)

    ref_uris = []
    for f in ref_files:
        raw = await f.read()
        ref_uris.append(resize_to_data_uri(raw))

    comment_bytes = await comment_file.read()
    filename      = comment_file.filename or ""

    background_tasks.add_task(run_full_scan, comment_bytes, filename, ref_uris, post_url)
    return {"status": "started", "filename": filename, "ref_count": len(ref_uris), "post_url": post_url}


@app.get("/results")
def get_results():
    return scan_state


@app.get("/")
def root():
    return {"message": "UGC Monitor API", "docs": "/docs"}
