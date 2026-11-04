#!/usr/bin/env python3
"""Drive the real recipe, compiler, and both warm graph caches; no mocks.

Alternate paths are real symlinks to the owning shell's inputs. Inspection
lowers production test edges without executing the corpus. --dry-run would
force provider refresh and mask the original ambient-getEnv defect.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


KEYS = ("CT_PCRE_LIB_DIR", "CT_ZSTD_INCLUDE_DIR", "CT_ZSTD_LIB_DIR",
        "CT_NIM_STEW_SRC", "CT_NIM_RESULTS_SRC", "CT_NIM_UNITTEST2_SRC")


def select(environment, manifest):
    result = environment.copy()
    assignments = [item for item in result.get("REPRO_VARIANTS", "").split(",")
                   if item and item.partition("=")[0].strip() != "toolPathsFile"]
    result["REPRO_VARIANTS"] = ",".join(assignments + ["toolPathsFile=" + str(manifest)])
    return result


def selection(raw):
    values = [item.partition("=")[2].strip() for item in raw.split(",")
              if item.partition("=")[0].strip() == "toolPathsFile"]
    assert len(values) == 1, raw
    return values[0]


def assert_path(graph, key, alternate):
    actions = {action["id"]: action for action in graph["actions"]}
    compile_action = actions["codetracer-trace-format-nim.test_build.test_path_filter"]
    if key in ("CT_PCRE_LIB_DIR", "CT_ZSTD_LIB_DIR"):
        assert "--passL:-L" + str(alternate) in compile_action["argv"], key
        assert "--passL:-Wl,-rpath," + str(alternate) in compile_action["argv"], key
        assert str(alternate) in compile_action["inputs"], key
    elif key == "CT_ZSTD_INCLUDE_DIR":
        assert "--passC:-I" + str(alternate) in compile_action["argv"], key
        assert str(alternate / "zstd.h") in compile_action["inputs"], key
        execute_action = actions["codetracer-trace-format-nim.test_execute.test_path_filter"]
        metadata = alternate.parent / "lib" / "pkgconfig"
        assert "PKG_CONFIG_PATH=" + str(metadata) in execute_action["env"], key
        assert str(metadata / "libzstd.pc") in execute_action["inputs"], key
    else:
        assert "--path:" + str(alternate) in compile_action["argv"], key
        assert str(alternate) in compile_action["inputs"], key


def assert_warm(graph):
    assert graph["providerGraphCacheHit"] and graph["loweredGraphCacheHit"], graph
    assert graph["providerInvocations"] == 0, graph


def main():
    if not __debug__:
        raise RuntimeError("run this assertion suite without Python optimization")
    checkout = Path(__file__).resolve().parent.parent
    repro = shutil.which("repro")
    assert repro, "enter the owning dev shell: repro is missing"
    original = {key: os.environ[key] for key in KEYS}
    for key, value in original.items():
        assert Path(value).is_dir(), f"invalid shell input: {key}"

    # Real re-entry of the owning shell must replace an inherited old default,
    # while preserving another project's variant assignments.
    inherited = os.environ.copy()
    inherited["REPRO_VARIANTS"] = "otherVariant=keep,toolPathsFile=/nonexistent-old"
    transport = subprocess.run(
        ["nix", "develop", "--command", "bash", "-c", 'printf "%s\\n" "$REPRO_VARIANTS"'],
        cwd=checkout, env=inherited, text=True, capture_output=True)
    assert transport.returncode == 0, transport.stdout + transport.stderr
    transported = transport.stdout.splitlines()[-1]
    assert "otherVariant=keep" in transported.split(","), transported
    current = Path(selection(transported))
    assert current.is_file() and str(current) != "/nonexistent-old", transported
    declared = json.loads(current.read_text())
    assert declared["schema"] == 1
    for key, value in original.items():
        assert declared["paths"][key] == value, key
    print("PASS: owning shell refreshes its manifest and preserves unrelated variants", flush=True)

    with tempfile.TemporaryDirectory(prefix="ct-repro-tool-paths-") as temporary:
        scratch = Path(temporary)
        project = scratch / "project"
        project.mkdir()
        for name in ("repro.nim", "build_writer_artifacts.nim", "build_ffi_flags.nim",
                     "build_ffi.nims", "codetracer_trace_format.nimble", "nim.cfg",
                     "config.nims", "src", "tests", "include"):
            source = checkout / name
            if source.is_dir():
                shutil.copytree(source, project / name)
            else:
                shutil.copy2(source, project / name)
        manifest = scratch / "paths.json"

        def write_paths(paths, destination=manifest):
            destination.write_text(json.dumps({"schema": 1, "paths": paths}))

        write_paths(original)
        environment = select(os.environ, manifest)
        environment["REPRO_PROVIDER_PARALLEL_BUILD"] = "2"

        def graph(env=environment, refusal=None):
            result = subprocess.run(
                [repro, "graph", ".#test", "--view=actions", "--format=json",
                 "--tool-provisioning=path"],
                cwd=project, env=env, text=True, capture_output=True)
            if refusal is not None:
                assert result.returncode != 0, "invalid manifest was ignored by a warm graph"
                assert refusal in result.stdout + result.stderr, result.stdout + result.stderr
                return None
            assert result.returncode == 0, result.stdout + result.stderr
            return json.loads(result.stdout)

        graph()
        assert_warm(graph())
        print("PASS: unchanged selection and contents reuse both graph caches", flush=True)
        alternates = {}
        for key in KEYS:
            print(f"Checking {key} against warm graph caches", flush=True)
            alternate = scratch / key
            if key == "CT_ZSTD_INCLUDE_DIR":
                alternate = alternate / "include"
                alternate.parent.mkdir()
                (alternate.parent / "lib").mkdir()
                (alternate.parent / "lib" / "pkgconfig").symlink_to(
                    Path(original[key]).parent / "lib" / "pkgconfig", target_is_directory=True)
            alternate.symlink_to(original[key], target_is_directory=True)
            alternates[key] = alternate
            changed = dict(original, **{key: str(alternate)})
            write_paths(changed)
            updated = graph()
            assert_path(updated, key, alternate)
            assert updated["providerInvocations"] > 0, f"{key}: provider did not refresh"
            assert_warm(graph())
            del changed[key]
            write_paths(changed)
            graph(refusal="owning tool-path manifest requires " + key)
            write_paths(original)
            graph()
            print(f"PASS: {key}: content edit updates flags/inputs; missing field is refused", flush=True)

        alternate_manifest = scratch / "alternate.json"
        write_paths(dict(original, CT_PCRE_LIB_DIR=str(alternates["CT_PCRE_LIB_DIR"])), alternate_manifest)
        selected = select(environment, alternate_manifest)
        assert_path(graph(selected), "CT_PCRE_LIB_DIR", alternates["CT_PCRE_LIB_DIR"])
        assert_warm(graph(selected))
        print("PASS: selecting another manifest changes the graph and warms independently", flush=True)

        # --variant is the explicit override after shell activation. A missing
        # selection must fail even when the environment selects a valid file.
        missing = scratch / "missing.json"
        override = subprocess.run(
            [repro, "build", ".#default", "--prepare-only", "--daemon=off",
             "--tool-provisioning=path", "--variant", "toolPathsFile=" + str(missing)],
            cwd=project, env=environment, text=True, capture_output=True)
        assert override.returncode != 0, "explicit --variant did not override shell selection"
        assert "owning builds require --variant toolPathsFile" in override.stdout + override.stderr
        graph(select(environment, ""), refusal="owning builds require --variant toolPathsFile")
        print("PASS: CLI override wins and missing selection has no ambient fallback", flush=True)

        for malformed in ([], {"schema": 1}, {"schema": "1", "paths": original},
                          {"schema": 1, "paths": []}):
            manifest.write_text(json.dumps(malformed))
            graph(refusal="invalid owning tool-path manifest schema")
        write_paths(dict(original, CT_PCRE_LIB_DIR=123))
        graph(refusal="owning tool-path manifest requires CT_PCRE_LIB_DIR")
        print("PASS: malformed metadata and nonstring paths are refused by name", flush=True)


if __name__ == "__main__":
    main()
