// Proposed immutable native inputs for the released RunQuota bootstrap.
// Native Windows mutation controls are required before this is adopted.
using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Microsoft.Win32.SafeHandles;
public sealed class QuotaBootstrapInputs : IDisposable {
    [StructLayout(LayoutKind.Sequential)] struct Info {
        public uint Attributes;
        public System.Runtime.InteropServices.ComTypes.FILETIME Creation, Access, Write;
        public uint Volume, SizeHigh, SizeLow, Links, IndexHigh, IndexLow;
    }
    [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
    static extern SafeFileHandle CreateFileW(string p, uint access, uint share, IntPtr sec, uint creation, uint flags, IntPtr template);
    [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
    static extern uint GetFinalPathNameByHandleW(SafeFileHandle h, StringBuilder p, uint length, uint flags);
    [DllImport("kernel32.dll", SetLastError=true)]
    static extern bool GetFileInformationByHandle(SafeFileHandle h, out Info info);
    readonly List<SafeFileHandle> handles = new List<SafeFileHandle>();
    readonly Dictionary<SafeFileHandle,string> paths = new Dictionary<SafeFileHandle,string>();
    readonly List<FileStream> files = new List<FileStream>();
    readonly Dictionary<string,string> hashes = new Dictionary<string,string>(StringComparer.OrdinalIgnoreCase);
    readonly Dictionary<string,FileStream> streams = new Dictionary<string,FileStream>(StringComparer.OrdinalIgnoreCase);
    readonly HashSet<string> directories = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    // Refusal diagnostics. Every message starts with its stable tag
    // ("quota-hold/<reason>") followed by ": " and the path and cause, so the
    // CI log names the object and, for Win32 failures, the error code and its
    // system text (an explicit Win32Exception message replaces that text, and
    // PowerShell prints only the message). Callers that test for one specific
    // refusal compare Exception.Data["tag"], which is exactly the tag.
    // https://learn.microsoft.com/windows/win32/debug/system-error-codes
    static string Show(string p) { return p == null ? "<null>" : "'" + p + "'"; }
    static InvalidDataException Refuse(string tag, string p, string detail = null) {
        var e = new InvalidDataException(tag + ": " + Show(p) + (detail == null ? "" : ": " + detail));
        e.Data["tag"] = tag; e.Data["path"] = p;
        return e;
    }
    static Win32Exception Win32(string tag, string p, int code, string detail = null) {
        string system = new Win32Exception(code).Message;
        var e = new Win32Exception(code, tag + ": " + Show(p) + ": win32 " + code + " (" + system + ")" + (detail == null ? "" : ": " + detail));
        e.Data["tag"] = tag; e.Data["path"] = p;
        return e;
    }
    static string Normal(string p) {
        if (String.IsNullOrEmpty(p) || !Path.IsPathFullyQualified(p)) throw Refuse("quota-hold/absolute-path", p);
        return Path.TrimEndingDirectorySeparator(Path.GetFullPath(p));
    }
    // Extended-length form of a fully qualified, already normalised path, for
    // the raw Win32 calls below. Without the \\?\ prefix CreateFileW refuses a
    // path longer than MAX_PATH (260) with ERROR_PATH_NOT_FOUND unless the host
    // sets LongPathsEnabled AND the process manifest is longPathAware; .NET's
    // own file APIs add the prefix internally, raw P/Invoke does not. Windows
    // does not normalise a \\?\ path, so this accepts only a path that is
    // already normal (callers pass Normal(p)) and refuses '/', '.', '..',
    // empty segments and trailing dots or spaces rather than open a literal
    // name the normal form does not denote. Comparisons and diagnostics stay
    // on the normal form. Pure string logic: no Path API, so it behaves the
    // same off-Windows (ci/test-win32-path-forms.ps1).
    // https://learn.microsoft.com/windows/win32/fileio/maximum-file-path-limitation
    // https://learn.microsoft.com/windows/win32/fileio/naming-a-file#win32-file-namespaces
    public static string Win32Path(string normal) {
        if (normal == null) throw new ArgumentNullException("normal");
        if (normal.StartsWith(@"\\?\", StringComparison.Ordinal)) return normal;
        string prefix, rest;
        if (normal.Length >= 3 && normal[1] == ':' && normal[2] == '\\' &&
            ((normal[0] >= 'A' && normal[0] <= 'Z') || (normal[0] >= 'a' && normal[0] <= 'z'))) {
            prefix = @"\\?\" + normal.Substring(0, 3); rest = normal.Substring(3);
        } else if (normal.StartsWith(@"\\", StringComparison.Ordinal) && normal.Length > 2 &&
            normal[2] != '?' && normal[2] != '.' && normal[2] != '\\') {
            // \\server\share\x -> \\?\UNC\server\share\x; server and share are required.
            prefix = @"\\?\UNC\"; rest = normal.Substring(2);
            if (rest.Split('\\').Length < 2) throw new ArgumentException("UNC path lacks a share: " + normal, "normal");
        } else throw new ArgumentException("not a fully qualified drive or UNC path: " + normal, "normal");
        if (rest.IndexOf('/') >= 0) throw new ArgumentException("forward slash in path: " + normal, "normal");
        if (rest.Length > 0) foreach (string part in rest.Split('\\'))
            if (part.Length == 0 || part == "." || part == ".." || part.EndsWith(".") || part.EndsWith(" "))
                throw new ArgumentException("path is not normalised: " + normal, "normal");
        return prefix + rest;
    }
    // `held` names the path the handle was opened for; it is used only in diagnostics.
    static string Final(SafeFileHandle h, string held) {
        var s = new StringBuilder(32768);
        uint n = GetFinalPathNameByHandleW(h, s, (uint)s.Capacity, 0);
        if (n == 0 || n >= s.Capacity) {
            int e = Marshal.GetLastWin32Error();
            throw Win32("quota-hold/final-path", held, e, n == 0 ? null : "final path needs " + n + " characters");
        }
        string p = s.ToString();
        if (p.StartsWith(@"\\?\UNC\", StringComparison.OrdinalIgnoreCase)) p = @"\\" + p.Substring(8);
        else if (p.StartsWith(@"\\?\", StringComparison.OrdinalIgnoreCase)) p = p.Substring(4);
        return Normal(p);
    }
    SafeFileHandle Open(string p, bool directory) {
        // GENERIC_READ participates in Windows sharing checks. Do not downgrade
        // this to FILE_READ_ATTRIBUTES, which would allow rename while held.
        var h = CreateFileW(Win32Path(Normal(p)), 0x80000000u, 1, IntPtr.Zero, 3,
            0x00200000u | (directory ? 0x02000000u : 0), IntPtr.Zero);
        if (h.IsInvalid) {
            int e = Marshal.GetLastWin32Error(); h.Dispose();
            throw Win32("quota-hold/open", p, e);
        }
        handles.Add(h); paths.Add(h, Normal(p));
        Info info;
        if (!GetFileInformationByHandle(h, out info)) throw Win32("quota-hold/identity", p, Marshal.GetLastWin32Error());
        if ((info.Attributes & 0x400) != 0 || ((info.Attributes & 0x10) != 0) != directory)
            throw Refuse("quota-hold/direct-kind", p, String.Format("attributes 0x{0:x}, expected a {1} without a reparse point", info.Attributes, directory ? "directory" : "file"));
        string final = Final(h, p);
        if (!String.Equals(Normal(p), final, StringComparison.OrdinalIgnoreCase)) throw Refuse("quota-hold/physical-path", p, "final path " + Show(final));
        return h;
    }
    public void HoldDirectory(string path) {
        path = Normal(path);
        if (directories.Add(path)) Open(path, true);
    }
    public void HoldFile(string root, string relative, string sha256) {
        root = Normal(root);
        if (String.IsNullOrEmpty(relative) || relative.Contains(":") || relative.Contains("\\")) throw Refuse("quota-hold/member-path", relative, "under " + Show(root));
        var parts = relative.Split('/');
        foreach (var part in parts) if (part.Length == 0 || part == "." || part == "..") throw Refuse("quota-hold/member-path", relative, "under " + Show(root));
        HoldDirectory(root);
        string path = root;
        for (int i=0; i<parts.Length-1; i++) { path = Path.Combine(path, parts[i]); HoldDirectory(path); }
        path = Path.Combine(path, parts[parts.Length-1]);
        if (streams.ContainsKey(path)) throw Refuse("quota-hold/duplicate-member", path);
        var h = Open(path, false);
        var stream = new FileStream(h, FileAccess.Read);
        files.Add(stream); streams.Add(path, stream); hashes.Add(path, sha256);
        VerifyFile(path, stream, sha256);
    }
    static void VerifyFile(string path, FileStream stream, string expected) {
        stream.Position = 0;
        string actual;
        using (var hash = SHA256.Create()) actual = BitConverter.ToString(hash.ComputeHash(stream)).Replace("-", "").ToLowerInvariant();
        stream.Position = 0;
        if (!String.Equals(actual, expected, StringComparison.Ordinal)) throw Refuse("quota-hold/member-bytes", path, "sha256 " + actual + ", expected " + expected);
        string final = Final(stream.SafeFileHandle, path);
        if (!String.Equals(Normal(path), final, StringComparison.OrdinalIgnoreCase)) throw Refuse("quota-hold/member-continuity", path, "final path " + Show(final));
    }
    public void Verify() {
        foreach (var entry in streams) VerifyFile(entry.Key, entry.Value, hashes[entry.Key]);
        foreach (var h in handles) {
            if (h.IsClosed || h.IsInvalid) throw Refuse("quota-hold/closed-handle", paths[h]);
            string final = Final(h, paths[h]);
            if (!String.Equals(paths[h], final, StringComparison.OrdinalIgnoreCase)) throw Refuse("quota-hold/path-continuity", paths[h], "final path " + Show(final));
        }
    }
    public void Dispose() {
        for (int i=files.Count-1;i>=0;i--) files[i].Dispose();
        for (int i=handles.Count-1;i>=0;i--) handles[i].Dispose();
    }
}
