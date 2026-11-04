# Realize the public SDK archive before the owning path-mode environment.
import repro_project_dsl

package trace_windows_nim_prerequisite:
  defaultToolProvisioning "tarball"
  uses:
    "nim >=2.2.10 <2.2.11"
  devEnv:
    discard
