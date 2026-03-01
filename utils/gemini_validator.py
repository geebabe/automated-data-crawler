import asyncio
import pandas as pd
import json
import os
from google import genai
from google.genai import types
from pydantic import BaseModel

# ======================
# CONFIG
# ======================
INPUT_FILE = "sggp_top20_unique_post_content.csv"
OUTPUT_FILE = "sggp/sggp_gemini_valid.csv"
MODEL_NAME = "gemini-3-flash-preview"   # ✅ correct choice
API_KEY_ENV = "GEMINI_API_KEY"


SYSTEM_INSTRUCTION = """Bạn là một chuyên gia lọc dữ liệu cho dự án nghiên cứu ViEVPolicy (Chuyển đổi xe máy xăng sang xe điện).

NHIỆM VỤ:
Phân tích văn bản đầu vào (Post Content và Comment) để xác định xem chủ đề "Xe máy điện / Chuyển đổi phương tiện / Giao thông xanh" có phải là TRỌNG TÂM THẢO LUẬN hay không.

QUY TẮC QUYẾT ĐỊNH (CRITICAL):
Output "true" (Có liên quan) CHỈ KHI nội dung đáp ứng tiêu chí "Trọng tâm" dưới đây.
Output "false" (Không liên quan) nếu nội dung rơi vào tiêu chí "Nhắc thoáng qua/Nhiễu".

1. TIÊU CHÍ "CÓ LIÊN QUAN" (Accept):
Nội dung phải BÀN LUẬN CỤ THỂ hoặc TRỰC TIẾP về:
- Chính sách: Các đề án cấm xe xăng, trợ giá xe điện, quy định khí thải tại VN.
- Phương tiện: So sánh xe xăng vs xe điện, review xe máy điện (VinFast, Dat Bike...), pin, trạm sạc.
- Shipper/Tài xế: Thảo luận về chi phí nhiên liệu, việc đổi xe để chạy app, chính sách hãng xe liên quan đến phương tiện.
- Môi trường: Ô nhiễm khói bụi từ xe máy, giải pháp giao thông xanh.
(Chấp nhận nếu Post không rõ ràng nhưng Comment thảo luận sôi nổi về các ý trên).

2. TIÊU CHÍ "LOẠI BỎ" (Reject - Kể cả khi có từ khóa):
- Nhắc thoáng qua (Incidental): Bài viết nói về chủ đề khác (ví dụ: Bất động sản, Chứng khoán, Tình yêu, Đời sống...) và chỉ nhắc đến "xe điện" hoặc "VinFast" như một ví dụ phụ hoặc bối cảnh làm nền.
- Ẩn dụ/So sánh: Dùng từ ngữ xe cộ để mô tả việc khác (VD: "Chạy nhanh như xe điện" để mô tả cầu thủ bóng đá).
- Tin tức tập đoàn chung chung: Bàn về cổ phiếu Vingroup, nhân sự VinFast, tỷ phú Phạm Nhật Vượng nhưng KHÔNG nói về sản phẩm xe hay chính sách xe.
- Shipper không liên quan xe: Shipper than phiền khách hàng, bom hàng, chuyện vui buồn nghề nghiệp KHÔNG liên quan đến xăng xe/phương tiện.
- Spam/Quảng cáo không liên quan: Bán sim, bán đất, vay vốn (dù có hashtag #vinfast).

INPUT DATA:
---
Post Content: {post_content}
---

OUTPUT FORMAT (JSON Only):
{
  "is_relevant": true/false,
  "reason": "Giải thích ngắn gọn (tại sao là trọng tâm hoặc tại sao bị loại)"
}"""


# ======================
# SCHEMA
# ======================
class ValidationResult(BaseModel):
    is_relevant: bool


# ======================
# GEMINI CLIENT
# ======================
client = genai.Client(api_key=os.environ.get(API_KEY_ENV))


async def validate_content(post: str) -> bool:
    prompt = f"""
Post Content:
{post}


"""

    for attempt in range(3):
        try:
            response = await client.aio.models.generate_content(
                model=MODEL_NAME,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_INSTRUCTION,
                    response_mime_type="application/json",
                    response_schema=ValidationResult,
                    temperature=0
                )
            )

            if response.parsed:
                return response.parsed.is_relevant

            if response.text:
                return json.loads(response.text).get("is_relevant", False)

        except Exception as e:
            print(f"[Retry {attempt+1}] Gemini error:", e)
            await asyncio.sleep(5)

    return False


# ======================
# MAIN
# ======================
async def main():
    df = pd.read_csv(INPUT_FILE)

    results = []

    for idx, row in df.iterrows():
        is_rel = await validate_content(
            str(row.get("post_content", "")),
            # str(row.get("comment_content", ""))
        )
        results.append(is_rel)

        if idx % 10 == 0:
            print(f"Processed {idx}: {is_rel}")

        await asyncio.sleep(1)  # ✅ very important for free tier

    df["is_relevant"] = results
    df_filtered = df[df["is_relevant"]]

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
