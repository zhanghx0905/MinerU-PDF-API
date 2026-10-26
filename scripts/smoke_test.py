"""Create a synthetic PDF and verify a running API, including cache separation."""

import argparse
import io
import json
import time

import httpx
from PIL import Image, ImageDraw
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas


def sample_pdf():
    output = io.BytesIO()
    pdf = canvas.Canvas(output)
    pdf.setFont("Helvetica-Bold", 22)
    pdf.drawString(60, 780, "MinerU API verification")
    pdf.setFont("Helvetica", 12)
    pdf.drawString(60, 750, "PDF parsing, tables and embedded images")
    for index, line in enumerate(
        [
            "Item          Quantity          Price",
            "Apple         2                 10",
            "Pear          3                 12",
        ]
    ):
        pdf.drawString(60, 710 - index * 24, line)
    image = Image.new("RGB", (600, 260), "white")
    draw = ImageDraw.Draw(image)
    draw.rectangle((5, 5, 590, 250), outline="black", width=3)
    draw.rectangle((60, 100, 140, 220), fill="blue")
    draw.rectangle((210, 60, 290, 220), fill="green")
    draw.rectangle((360, 30, 440, 220), fill="orange")
    draw.text((40, 12), "SALES CHART: A / B / C", fill="black", font_size=22)
    pdf.drawImage(ImageReader(image), 60, 440, width=420, height=182)
    # A non-chart illustration ensures the image-embedding option is exercised
    # even when the chart above is rendered as a structured table.
    illustration = Image.new("RGB", (400, 180), "skyblue")
    landscape = ImageDraw.Draw(illustration)
    landscape.ellipse((280, 15, 340, 75), fill="orange")
    landscape.polygon([(0, 180), (110, 40), (220, 180)], fill="forestgreen")
    landscape.polygon([(130, 180), (290, 65), (400, 180)], fill="darkgreen")
    pdf.drawImage(ImageReader(illustration), 60, 260, width=360, height=162)
    pdf.save()
    return output.getvalue()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="API base URL, e.g. http://127.0.0.1:3888")
    parser.add_argument("--timeout", type=float, default=1200)
    args = parser.parse_args()
    payload = sample_pdf()
    with httpx.Client(
        base_url=args.url.rstrip("/"), timeout=args.timeout, trust_env=False
    ) as client:
        health = client.get("/health")
        health.raise_for_status()
        assert health.json()["mineru_version"] == "4.0.12", health.text
        results = []
        for encode_img in (True, True, False):
            started = time.perf_counter()
            response = client.post(
                "/v2/parse/file",
                params={"encode_img": str(encode_img).lower()},
                files={"file": ("verification.pdf", payload, "application/pdf")},
            )
            response.raise_for_status()
            data = response.json()
            assert data["success"] is True, data
            assert data["pages"] == 1, data
            assert "verification" in data["markdown"].lower(), data["markdown"]
            if not encode_img:
                assert "data:image/" not in data["markdown"]
            results.append(
                {
                    "encode_img": encode_img,
                    "seconds": round(time.perf_counter() - started, 3),
                    "markdown_bytes": len(data["markdown"].encode()),
                    "contains_image": "data:image/" in data["markdown"],
                }
            )
        assert results[0]["contains_image"], "Synthetic chart image is absent from encoded output"
        assert client.get("/health").json()["completed"] >= 2
        print(json.dumps({"health": health.json(), "requests": results}, indent=2))


if __name__ == "__main__":
    main()
