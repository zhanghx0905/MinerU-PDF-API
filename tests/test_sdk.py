"""Exercise the installed MinerU SDK using its native-text flash path."""

from app.engine import MinerUEngine, markdown_without_images
from scripts.smoke_test import sample_pdf


def test_installed_sdk_renders_real_pdf_and_suppresses_images(monkeypatch, tmp_path):
    monkeypatch.setenv("MINERU_HOME", str(tmp_path))
    monkeypatch.setenv("MINERU_MODEL_SOURCE", "local")
    engine = MinerUEngine("flash")
    try:
        engine.warmup()
        encoded = engine.parse(sample_pdf(), True)
        plain = engine.parse(sample_pdf(), False)
        assert encoded.pages == plain.pages == 1
        assert "verification" in encoded.markdown.lower()
        assert "verification" in plain.markdown.lower()
        assert "data:image/" in encoded.markdown
        assert "data:image/" not in plain.markdown
    finally:
        engine.close()


def test_no_image_rendering_handles_table_chart_fallbacks_without_losing_text():
    from mineru import MiddleJson, ParseResult

    image = "data:image/png;base64,aGVsbG8="
    table = f'<table><tr><td>Apple</td><td>10<img src="{image}"></td></tr></table>'
    blocks = [
        {
            "type": "table",
            "content": [{"type": "table_body", "content": "", "image_path": "fallback.png"}],
        },
        {
            "type": "chart",
            "content": [{"type": "chart_body", "content": "", "image_base64": image}],
        },
        {
            "type": "chart",
            "content": [{"type": "chart_body", "content": table, "image_base64": image}],
        },
        {
            "type": "table",
            "content": [{"type": "table_body", "content": table.replace("Apple", "Pear")}],
        },
        {
            "type": "image",
            "content": [
                {
                    "type": "image_body",
                    "content": f'<p>Recognized description<img src="{image}"></p>',
                    "image_base64": image,
                }
            ],
        },
        {"type": "equation", "content": "E=mc^2", "image_base64": image},
    ]
    for index, block in enumerate(blocks):
        block["index"] = index
        block["bbox"] = [0, index / 10, 1, (index + 1) / 10]
        if isinstance(block["content"], list):
            block["content"][0]["index"] = index
            block["content"][0]["bbox"] = block["bbox"]
    result = ParseResult(
        MiddleJson.from_dict(
            {
                "schema": "docvortex.middle",
                "schema_version": "2.0",
                "metadata": {
                    "file_suffix": "pdf",
                    "producer": {"name": "mineru", "version": "4.0.12"},
                },
                "pages": [{"page_idx": 0, "blocks": blocks}],
                "is_full_document": True,
            }
        )
    )
    encoded = result.markdown()
    plain = markdown_without_images(result)
    assert "data:image/" in encoded
    assert "data:image/" not in plain
    assert "fallback.png" not in plain
    assert "<img" not in plain.lower()
    for expected in ("Apple", "Pear", "10", "Recognized description", "E=mc^2"):
        assert expected in plain
    assert result.markdown() == encoded  # Rendering must not mutate the SDK result.
