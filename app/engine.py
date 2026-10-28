"""Reuse lightweight parsers and shared models while keeping request state isolated."""

from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Lock

from .metrics import record_sdk_stages, timed


@dataclass(frozen=True)
class ParseOutput:
    markdown: str
    pages: int
    timings: dict[str, float] = field(default_factory=dict)


def markdown_without_images(result):
    """Remove image assets before rendering, including SDK table/chart fallbacks."""
    from bs4 import BeautifulSoup
    from mineru.render import render_markdown
    from mineru.types import BlockBase, ImagePayloadBlock, ImagePayloadContentBlock

    document = result.middle_json.model_copy(deep=True)
    pending = [block for page in document.pages for block in page.blocks]
    while pending:
        block = pending.pop()
        if isinstance(block, ImagePayloadBlock):
            block.image_base64 = block.image_path = block.image_url = None
        if isinstance(block, ImagePayloadContentBlock) and "<img" in block.content.lower():
            fragment = BeautifulSoup(block.content, "html.parser")
            for image in fragment.find_all("img"):
                image.decompose()
            block.content = str(fragment)
        content = getattr(block, "content", None)
        if isinstance(content, list):
            pending.extend(child for child in content if isinstance(child, BlockBase))
    return render_markdown(document, image_renderer=lambda _block: "")


class MinerUEngine:
    def __init__(self, tier: str):
        self.tier = tier
        self._parsers = {}
        self._parser_lock = Lock()

    def warmup(self):
        # Generate a tiny document at startup instead of keeping a private sample
        # PDF in the repository or charging initialization to the first caller.
        from PIL import Image, ImageDraw
        from reportlab.lib.utils import ImageReader
        from reportlab.pdfgen import canvas

        image = Image.new("RGB", (240, 80), "white")
        ImageDraw.Draw(image).text((10, 20), "MinerU warmup", fill="black", font_size=20)
        output = BytesIO()
        document = canvas.Canvas(output)
        document.setFont("Helvetica", 11)
        for line in range(18):
            document.drawString(
                60,
                740 - line * 18,
                "MinerU service warmup: initialize the parser before accepting online traffic.",
            )
        document.drawImage(ImageReader(image), 60, 300, width=240, height=80)
        formula = document.beginText(220, 405)
        formula.setFont("Times-Italic", 24)
        formula.textOut("E = mc")
        formula.setRise(10)
        formula.setFont("Times-Italic", 12)
        formula.textOut("2")
        document.drawText(formula)
        document.save()
        self.parse(output.getvalue(), True)

    def parse(self, pdf_bytes: bytes, encode_img: bool, tier: str | None = None) -> ParseOutput:
        # Public MinerU 4 SDK; model loading is lazy so HTTP startup is inexpensive.
        tier = tier or self.tier
        with self._parser_lock:
            if tier not in self._parsers:
                from mineru import MinerUParser

                self._parsers[tier] = MinerUParser(tier=tier, image_analysis=True)
            parser = self._parsers[tier]

        # This directory lives until model execution actually finishes, including
        # when the HTTP caller has already timed out or disconnected.
        with TemporaryDirectory(prefix="mineru-") as directory:
            path = Path(directory) / "document.pdf"
            path.write_bytes(pdf_bytes)
            timings = {}
            with timed(timings, "sdk.total"), record_sdk_stages(timings):
                result = parser.parse(path)
            with timed(timings, "markdown.render"):
                markdown = result.markdown() if encode_img else markdown_without_images(result)
            return ParseOutput(markdown=markdown, pages=len(result.pages), timings=timings)

    def close(self):
        for parser in self._parsers.values():
            parser.close()
