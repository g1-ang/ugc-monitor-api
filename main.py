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

import os, time, json, base64, requests, tempfile
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

import gspread
from google.oauth2.service_account import Credentials

load_dotenv()

# ── 설정 ───────────────────────────────────────
APIFY_API_TOKEN    = os.getenv("APIFY_API_TOKEN")
SPREADSHEET_ID     = os.getenv("SPREADSHEET_ID")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS_PATH", "config/google_credentials.json")
GEMINI_API_KEY     = os.getenv("GEMINI_API_KEY")
MY_IG_ID           = "pitapat_prompt"
SHEET_TAB_NAME     = "ugc_users"

APIFY_BASE       = "https://api.apify.com/v2"
ACTOR_COMMENTS   = "apify~instagram-comment-scraper"
ACTOR_PROFILE    = "apify~instagram-profile-scraper"
GEMINI_URL       = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"

app = FastAPI(title="UGC Monitor API")

# CORS — Vercel 프론트에서 호출 허용
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 스캔 상태 저장 (메모리)
scan_state = {
    "status": "idle",       # idle / running / done / error
    "progress": 0,
    "step": "",
    "results": [],
    "stats": {"feed": 0, "story": 0, "profile": 0},
    "started_at": None,
}


# ── Google Sheets 연결 ─────────────────────────
def get_sheet():
    creds = Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS,
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds).open_by_key(SPREADSHEET_ID).worksheet(SHEET_TAB_NAME)


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


# ── Gemini Vision 판별 ─────────────────────────
def analyze_with_reference(target_url: str, reference_b64: str, reference_mime: str) -> bool:
    """레퍼런스 이미지와 타겟 이미지를 비교해서 스타일 유사도 판별"""

    # 타겟 이미지 다운로드
    try:
        r = requests.get(target_url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        target_mime = r.headers.get("Content-Type", "image/jpeg").split(";")[0]
        target_b64  = base64.b64encode(r.content).decode("utf-8")
    except:
        return False

    payload = {
        "contents": [{
            "parts": [
                {"text": "아래 두 이미지를 비교해주세요.\n\n[이미지 1]은 레퍼런스 스타일 샘플입니다.\n[이미지 2]는 판별 대상입니다.\n\n판별 기준:\n- 두 이미지가 비슷한 AI 생성 스타일인가?\n- 비슷한 인물 표현 방식(얼굴 비율, 피부 보정, 분위기)인가?\n- 같은 AI 프롬프트나 도구로 만들었을 가능성이 있는가?\n\n반드시 YES 또는 NO 한 단어만 답하세요."},
                {"text": "[이미지 1] 레퍼런스:"},
                {"inline_data": {"mime_type": reference_mime, "data": reference_b64}},
                {"text": "[이미지 2] 판별 대상:"},
                {"inline_data": {"mime_type": target_mime,   "data": target_b64}},
            ]
        }],
        "generationConfig": {"temperature": 0.1, "maxOutputTokens": 10},
    }

    try:
        resp = requests.post(
            f"{GEMINI_URL}?key={GEMINI_API_KEY}",
            json=payload, timeout=30,
        )
        resp.raise_for_status()
        answer = resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip().upper()
        return "YES" in answer
    except:
        return False


# ── 전체 스캔 파이프라인 ───────────────────────
def run_full_scan(post_url: str, reference_b64: str, reference_mime: str):
    global scan_state
    scan_state.update({"status": "running", "progress": 5, "step": "댓글 수집 중...", "results": [], "started_at": datetime.now().isoformat()})

    try:
        sheet = get_sheet()

        # ── Phase 1: 댓글 수집 ──────────────────
        scan_state.update({"progress": 10, "step": "댓글 유저 수집 중..."})
        comments = run_apify(ACTOR_COMMENTS, {"directUrls": [post_url], "resultsLimit": 500})

        user_map = {}
        for c in comments:
            u = c.get("ownerUsername") or c.get("username") or (c.get("owner") or {}).get("username")
            if u and u.lower() != MY_IG_ID.lower():
                user_map[u] = c.get("postUrl") or post_url

        # 중복 제거 후 Sheets 저장
        existing = {r[0].strip() for r in sheet.get_all_values()[1:] if r and r[0]}
        to_add   = {u: v for u, v in user_map.items() if u not in existing}
        if to_add:
            now  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            rows = [[u,"",now,"","","","","none","",v,""] for u, v in sorted(to_add.items())]
            sheet.append_rows(rows, value_input_option="RAW")

        scan_state.update({"progress": 30, "step": f"프로필 스캔 중... ({len(user_map)}명)"})

        # ── Phase 2: 프로필 스캔 ────────────────
        usernames = list(user_map.keys())
        profiles  = []
        chunks    = [usernames[i:i+50] for i in range(0, len(usernames), 50)]

        for idx, chunk in enumerate(chunks):
            p = run_apify(ACTOR_PROFILE, {"usernames": chunk, "resultsLimit": 1})
            profiles.extend(p)
            progress = 30 + int((idx+1) / len(chunks) * 30)
            scan_state.update({"progress": progress, "step": f"프로필 스캔 중... ({(idx+1)*50}/{len(usernames)}명)"})

        profile_map = {}
        for p in profiles:
            uname = p.get("username", "")
            if uname:
                profile_map[uname.lower()] = p

        # 변화 감지
        candidates = []
        for uname, p in profile_map.items():
            has_story    = p.get("hasPublicStory", False)
            latest_posts = p.get("latestPosts") or p.get("posts") or []
            feed_url     = latest_posts[0].get("url", "") if latest_posts else ""
            profile_url  = p.get("profilePicUrl", "")

            if has_story or feed_url or profile_url:
                candidates.append({
                    "username":    uname,
                    "has_story":   has_story,
                    "has_feed":    bool(feed_url),
                    "feed_url":    feed_url,
                    "profile_url": profile_url,
                })

        scan_state.update({"progress": 65, "step": f"AI 이미지 판별 중... (0/{len(candidates)}명)"})

        # ── Phase 3: Gemini 판별 ────────────────
        confirmed = []
        for i, user in enumerate(candidates):
            uname = user["username"]

            # 판별 이미지 우선순위
            img_url  = user.get("feed_url") or user.get("profile_url")
            img_type = "feed" if user.get("feed_url") else ("story" if user.get("has_story") else "profile")

            if not img_url:
                continue

            is_ugc = analyze_with_reference(img_url, reference_b64, reference_mime)

            if is_ugc:
                confirmed.append({
                    "username":    uname,
                    "detected_at": datetime.now().strftime("%H:%M"),
                    "type":        img_type,
                    "link":        user.get("feed_url") or None,
                })

                # Sheets 업데이트
                all_vals = sheet.get_all_values()
                headers  = all_vals[0]
                for row_i, row in enumerate(all_vals[1:], start=2):
                    if row and row[0].strip().lower() == uname.lower():
                        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                        sheet.spreadsheet.values_batch_update({
                            "valueInputOption": "RAW",
                            "data": [
                                {"range": f"G{row_i}", "values": [["TRUE"]]},
                                {"range": f"H{row_i}", "values": [[img_type]]},
                                {"range": f"I{row_i}", "values": [[now]]},
                            ]
                        })
                        break

            progress = 65 + int((i+1) / max(len(candidates), 1) * 30)
            scan_state.update({
                "progress": progress,
                "step": f"AI 이미지 판별 중... ({i+1}/{len(candidates)}명)"
            })
            time.sleep(0.8)

        # ── 완료 ────────────────────────────────
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
    """스캔 시작 — URL + 레퍼런스 이미지 받아서 백그라운드 실행"""
    if scan_state["status"] == "running":
        return JSONResponse({"error": "이미 스캔이 진행 중입니다."}, status_code=409)

    # 레퍼런스 이미지 읽기
    img_bytes  = await reference_image.read()
    img_b64    = base64.b64encode(img_bytes).decode("utf-8")
    img_mime   = reference_image.content_type or "image/jpeg"

    background_tasks.add_task(run_full_scan, post_url, img_b64, img_mime)
    return {"status": "started", "post_url": post_url}


@app.get("/results")
def get_results():
    """현재 스캔 상태 및 결과 반환"""
    return scan_state


@app.get("/")
def root():
    return {"message": "UGC Monitor API", "docs": "/docs"}
