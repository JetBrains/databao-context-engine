from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import cast


import nemory.build_sources.internal.build_runner as build_runner
from nemory.pluginlib.build_plugin import BuildExecutionResult, BuildPlugin
from tests.utils.factories import make_run


def test_build_no_sources_exits_early(
    mocker,
    tmp_path: Path,
    run_repo,
    datasource_run_repo,
    chunk_embedding_service,
    stub_sources,
    stub_plugins,
):
    stub_sources([])
    stub_plugins({})

    process_spy = mocker.patch.object(build_runner, "process_source")
    run_create_spy = mocker.spy(run_repo, "create")
    run_update_spy = mocker.spy(run_repo, "update")
    create_dir_spy = mocker.patch.object(build_runner, "create_run_dir")

    build_runner.build(
        project_dir=tmp_path,
        chunk_embedding_service=chunk_embedding_service,
        run_repo=run_repo,
        datasource_run_repo=datasource_run_repo,
        project_id="proj",
        nemory_version=None,
    )

    process_spy.assert_not_called()
    run_create_spy.assert_not_called()
    run_update_spy.assert_not_called()
    create_dir_spy.assert_not_called()


def test_build_single_source(
    mocker,
    tmp_path: Path,
    run_repo,
    datasource_run_repo,
    chunk_embedding_service,
    stub_sources,
    stub_plugins,
    fake_run_dir,
    FakeSourceClass,
):
    src = FakeSourceClass(tmp_path / "src/files/file.md", "files", "md")
    stub_sources([src])
    stub_sources([src])
    mocker.patch.object(build_runner, "full_type_of", return_value="files/md")
    fake_plugin = object()
    stub_plugins({"files/md": fake_plugin})

    process_spy = mocker.patch.object(build_runner, "process_source")

    build_runner.build(
        project_dir=tmp_path,
        chunk_embedding_service=chunk_embedding_service,
        run_repo=run_repo,
        datasource_run_repo=datasource_run_repo,
        project_id="proj-123",
        nemory_version="1.0.0",
    )

    process_spy.assert_called_once()
    kwargs = process_spy.call_args.kwargs
    assert kwargs["plugin"] is fake_plugin
    assert kwargs["source"] is src
    assert kwargs["run_dir"] == fake_run_dir
    assert isinstance(kwargs["run_id"], int)


def test_build_skips_when_no_plugin_for_type(
    mocker,
    tmp_path: Path,
    run_repo,
    datasource_run_repo,
    chunk_embedding_service,
    stub_sources,
    stub_plugins,
    fake_run_dir,
    FakeSourceClass,
):
    src = FakeSourceClass(tmp_path / "src/databases/conf.yaml", "databases", "postgres")
    stub_sources([src])
    mocker.patch.object(build_runner, "full_type_of", return_value="databases/postgres")
    stub_plugins({})

    process_spy = mocker.patch.object(build_runner, "process_source")

    build_runner.build(
        project_dir=tmp_path,
        chunk_embedding_service=chunk_embedding_service,
        run_repo=run_repo,
        datasource_run_repo=datasource_run_repo,
        project_id="proj",
        nemory_version=None,
    )

    process_spy.assert_not_called()


def test_build_continues_on_process_source_error(
    mocker,
    tmp_path: Path,
    run_repo,
    datasource_run_repo,
    chunk_embedding_service,
    stub_sources,
    stub_plugins,
    fake_run_dir,
    FakeSourceClass,
):
    s1 = FakeSourceClass(tmp_path / "src/files/a.md", "files", "md")
    s2 = FakeSourceClass(tmp_path / "src/files/b.md", "files", "md")
    stub_sources([s1, s2])
    mocker.patch.object(build_runner, "full_type_of", return_value="files/md")
    stub_plugins({"files/md": object()})

    def flaky(**kwargs):
        if kwargs["source"] is s1:
            raise RuntimeError("boom")

    process_mock = mocker.patch.object(build_runner, "process_source", side_effect=flaky)

    build_runner.build(
        project_dir=tmp_path,
        chunk_embedding_service=chunk_embedding_service,
        run_repo=run_repo,
        datasource_run_repo=datasource_run_repo,
        project_id="proj",
        nemory_version=None,
    )

    assert process_mock.call_count == 2


def test_process_source(
    mocker, tmp_path: Path, run_repo, datasource_run_repo, chunk_embedding_service, FakeSourceClass
):
    src = FakeSourceClass(tmp_path / "src/files/file.md", "files", "md")
    plugin = _DummyPlugin()

    res = _result_now()
    mocker.patch.object(build_runner, "execute", return_value=res)

    chunks = [SimpleNamespace(embeddable_text="a", content="A"), SimpleNamespace(embeddable_text="b", content="B")]
    mocker.patch.object(build_runner, "divide_into_chunks", return_value=chunks)

    exp_single = mocker.patch.object(build_runner, "export_build_result")
    exp_all = mocker.patch.object(build_runner, "append_result_to_all_results")
    create_spy = mocker.spy(datasource_run_repo, "create")

    run = make_run(run_repo)
    build_runner.process_source(
        source=src,
        plugin=cast(BuildPlugin, plugin),
        datasource_run_repo=datasource_run_repo,
        run_id=run.run_id,
        run_dir=tmp_path / "out/run-x",
        chunk_embedding_service=chunk_embedding_service,
    )

    exp_single.assert_called_once_with(tmp_path / "out/run-x", res)
    exp_all.assert_called_once_with(tmp_path / "out/run-x", res)

    create_spy.assert_called_once()
    ds_dto = create_spy.spy_return

    chunk_embedding_service.embed_chunks.assert_called_once()
    _, kwargs = chunk_embedding_service.embed_chunks.call_args
    assert kwargs["datasource_run_id"] == ds_dto.datasource_run_id
    assert kwargs["chunks"] == chunks


def test_process_source_no_chunks_skips_embedding(
    mocker, tmp_path: Path, run_repo, datasource_run_repo, chunk_embedding_service, FakeSourceClass
):
    src = FakeSourceClass(tmp_path / "src/files/file.md", "files", "md")
    plugin = _DummyPlugin()

    res = _result_now()
    mocker.patch.object(build_runner, "execute", return_value=res)
    mocker.patch.object(build_runner, "divide_into_chunks", return_value=[])

    exp_single = mocker.patch.object(build_runner, "export_build_result")
    exp_all = mocker.patch.object(build_runner, "append_result_to_all_results")

    create_spy = mocker.spy(datasource_run_repo, "create")

    build_runner.process_source(
        source=src,
        plugin=cast(BuildPlugin, plugin),
        datasource_run_repo=datasource_run_repo,
        run_id=123,
        run_dir=tmp_path / "out/run-y",
        chunk_embedding_service=chunk_embedding_service,
    )

    exp_single.assert_called_once_with(tmp_path / "out/run-y", res)
    exp_all.assert_called_once_with(tmp_path / "out/run-y", res)

    create_spy.assert_not_called()
    chunk_embedding_service.embed_chunks.assert_not_called()


class _DummyPlugin:
    name = "dummy"

    def supported_types(self) -> set[str]:
        return {"files/md"}


def _result_now():
    return BuildExecutionResult(
        id="src-id",
        name="name",
        type="files/md",
        description=None,
        version="1",
        executed_at=datetime.now() - timedelta(seconds=1),
        result={"k": "v"},
    )
