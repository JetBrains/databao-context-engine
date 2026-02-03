from io import BufferedReader, BytesIO
from typing import Any

from docling.datamodel.base_models import DocumentStream, InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption

from databao_context_engine import BuildFilePlugin
from databao_context_engine.pluginlib.build_plugin import EmbeddableChunk
from databao_context_engine.services.docling_chunker import DoclingChunker


class PDFPlugin(BuildFilePlugin):
    id = "jetbrains/pdf"
    name = "PDF Plugin"

    def supported_types(self) -> set[str]:
        return {"files/pdf"}

    def build_file_context(self, full_type: str, file_name: str, file_buffer: BufferedReader) -> Any:
        pdf_bytes = file_buffer.read()

        opts = PdfPipelineOptions()
        opts.do_ocr = False
        opts.do_picture_description = False
        opts.do_picture_classification = False
        opts.generate_page_images = False
        opts.generate_picture_images = False

        converter = DocumentConverter(format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=opts)})
        stream = DocumentStream(name=file_name, stream=BytesIO(pdf_bytes))
        return converter.convert(stream).document

    def divide_context_into_chunks(self, context: Any) -> list[EmbeddableChunk]:
        indexer = DoclingChunker()
        return indexer.index(context)
