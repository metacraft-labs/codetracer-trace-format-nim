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
    static string Normal(string p) {
        if (String.IsNullOrEmpty(p) || !Path.IsPathFullyQualified(p)) throw new InvalidDataException("quota-hold/absolute-path");
        return Path.TrimEndingDirectorySeparator(Path.GetFullPath(p));
    }
    static string Final(SafeFileHandle h) {
        var s = new StringBuilder(32768);
        uint n = GetFinalPathNameByHandleW(h, s, (uint)s.Capacity, 0);
        if (n == 0 || n >= s.Capacity) throw new Win32Exception(Marshal.GetLastWin32Error(), "quota-hold/final-path");
        string p = s.ToString();
        if (p.StartsWith(@"\\?\UNC\", StringComparison.OrdinalIgnoreCase)) p = @"\\" + p.Substring(8);
        else if (p.StartsWith(@"\\?\", StringComparison.OrdinalIgnoreCase)) p = p.Substring(4);
        return Normal(p);
    }
    SafeFileHandle Open(string p, bool directory) {
        // GENERIC_READ participates in Windows sharing checks. Do not downgrade
        // this to FILE_READ_ATTRIBUTES, which would allow rename while held.
        var h = CreateFileW(p, 0x80000000u, 1, IntPtr.Zero, 3,
            0x00200000u | (directory ? 0x02000000u : 0), IntPtr.Zero);
        if (h.IsInvalid) { int e = Marshal.GetLastWin32Error(); h.Dispose(); throw new Win32Exception(e, "quota-hold/open"); }
        handles.Add(h); paths.Add(h, Normal(p));
        Info info;
        if (!GetFileInformationByHandle(h, out info)) throw new Win32Exception(Marshal.GetLastWin32Error(), "quota-hold/identity");
        if ((info.Attributes & 0x400) != 0 || ((info.Attributes & 0x10) != 0) != directory)
            throw new InvalidDataException("quota-hold/direct-kind");
        if (!String.Equals(Normal(p), Final(h), StringComparison.OrdinalIgnoreCase)) throw new InvalidDataException("quota-hold/physical-path");
        return h;
    }
    public void HoldDirectory(string path) {
        path = Normal(path);
        if (directories.Add(path)) Open(path, true);
    }
    public void HoldFile(string root, string relative, string sha256) {
        root = Normal(root);
        if (String.IsNullOrEmpty(relative) || relative.Contains(":") || relative.Contains("\\")) throw new InvalidDataException("quota-hold/member-path");
        var parts = relative.Split('/');
        foreach (var part in parts) if (part.Length == 0 || part == "." || part == "..") throw new InvalidDataException("quota-hold/member-path");
        HoldDirectory(root);
        string path = root;
        for (int i=0; i<parts.Length-1; i++) { path = Path.Combine(path, parts[i]); HoldDirectory(path); }
        path = Path.Combine(path, parts[parts.Length-1]);
        if (streams.ContainsKey(path)) throw new InvalidDataException("quota-hold/duplicate-member");
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
        if (!String.Equals(actual, expected, StringComparison.Ordinal)) throw new InvalidDataException("quota-hold/member-bytes");
        if (!String.Equals(Normal(path), Final(stream.SafeFileHandle), StringComparison.OrdinalIgnoreCase)) throw new InvalidDataException("quota-hold/member-continuity");
    }
    public void Verify() {
        foreach (var entry in streams) VerifyFile(entry.Key, entry.Value, hashes[entry.Key]);
        foreach (var h in handles) {
            if (h.IsClosed || h.IsInvalid) throw new InvalidDataException("quota-hold/closed-handle");
            if (!String.Equals(paths[h], Final(h), StringComparison.OrdinalIgnoreCase)) throw new InvalidDataException("quota-hold/path-continuity");
        }
    }
    public void Dispose() {
        for (int i=files.Count-1;i>=0;i--) files[i].Dispose();
        for (int i=handles.Count-1;i>=0;i--) handles[i].Dispose();
    }
}
