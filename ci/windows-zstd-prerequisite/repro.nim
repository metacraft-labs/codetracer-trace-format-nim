# Provision the authenticated SDK package before the owning graph needs it.
import repro_project_dsl

package trace_windows_zstd_prerequisite:
  defaultToolProvisioning "tarball"
  uses:
    "zstd >=1.5.6 <1.5.7"
  devEnv:
    discard
