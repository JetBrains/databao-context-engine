import sys

from databao_context_engine.plugins.files.pdf_plugin import PDFPlugin


def test_pdf_plugin_does_not_import_docling_on_import_or_init():
    PDFPlugin()
    assert "docling" not in sys.modules
    assert "docling_core" not in sys.modules


def test_context_type_triggers_docling_core_import():
    p = PDFPlugin()
    _ = p.context_type
    assert "docling_core" in sys.modules


def test_build_file_context_triggers_docling_import(tmp_path):
    p = PDFPlugin()
    assert "docling" not in sys.modules

    import io

    buf = io.BufferedReader(io.BytesIO(b"%PDF-1.4\n%fake\n"))
    try:
        p.build_file_context("pdf", "x.pdf", buf)
    except Exception:
        pass

    assert "docling" in sys.modules
