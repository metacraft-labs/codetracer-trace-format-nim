// A read-only binding to the SDK-selected Windows Nim payload. Handles deny
// write/delete sharing until the original producer/compiler probes finish.
// https://learn.microsoft.com/windows/win32/api/fileapi/nf-fileapi-createfilew
// https://learn.microsoft.com/windows/win32/api/fileapi/nf-fileapi-getfinalpathnamebyhandlew
using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Microsoft.Win32.SafeHandles;

public sealed class NimPayloadBinding : IDisposable {
    [StructLayout(LayoutKind.Sequential)] struct FileInfo {
        public uint Attributes;
        public System.Runtime.InteropServices.ComTypes.FILETIME Creation, Access, Write;
        public uint Volume, SizeHigh, SizeLow, Links, IndexHigh, IndexLow;
    }
    [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
    static extern SafeFileHandle CreateFileW(string path, uint access, uint share,
        IntPtr security, uint creation, uint flags, IntPtr template);
    [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
    static extern uint GetFinalPathNameByHandleW(SafeFileHandle handle,
        StringBuilder path, uint length, uint flags);
    [DllImport("kernel32.dll", SetLastError=true)]
    static extern bool GetFileInformationByHandle(SafeFileHandle handle, out FileInfo info);
    [DllImport("kernel32.dll", SetLastError=true)]
    static extern bool DeviceIoControl(SafeFileHandle handle, uint control,
        IntPtr input, uint inputSize, byte[] output, uint outputSize,
        out uint returned, IntPtr overlapped);

    readonly List<SafeFileHandle> handles = new List<SafeFileHandle>();
    readonly List<FileStream> files = new List<FileStream>();
    SafeFileHandle junction;
    FileStream image, header;
    string prefix, alias;
    public string ImagePath { get; private set; }
    public string IncludePath { get; private set; }
    public string ImageSha256 { get; private set; }
    public string HeaderSha256 { get; private set; }

    // Refusal diagnostics. Every message starts with its stable reason (for
    // example "missing absolute Nim payload role") followed by ": " and the
    // path and cause, so the CI log names the object and, for Win32 failures,
    // the error code and its system text (an explicit Win32Exception message
    // replaces that text, and PowerShell prints only the message). Callers
    // that test for one specific refusal compare Exception.Data["tag"], which
    // is exactly the reason.
    // https://learn.microsoft.com/windows/win32/debug/system-error-codes
    static string Show(string path) { return path == null ? "<null>" : "'" + path + "'"; }
    static InvalidDataException Refuse(string tag, string path, string detail = null) {
        var e = new InvalidDataException(tag + ": " + Show(path) + (detail == null ? "" : ": " + detail));
        e.Data["tag"] = tag; e.Data["path"] = path;
        return e;
    }
    static Win32Exception Win32(string tag, string path, int code, string detail = null) {
        string system = new Win32Exception(code).Message;
        var e = new Win32Exception(code, tag + ": " + Show(path) + ": win32 " + code + " (" + system + ")" + (detail == null ? "" : ": " + detail));
        e.Data["tag"] = tag; e.Data["path"] = path;
        return e;
    }
    static string Normal(string path) {
        if (String.IsNullOrEmpty(path) || !Path.IsPathFullyQualified(path))
            throw Refuse("missing absolute Nim payload role", path);
        return Path.TrimEndingDirectorySeparator(Path.GetFullPath(path));
    }
    static bool Same(string a, string b) {
        return String.Equals(Normal(a), Normal(b), StringComparison.OrdinalIgnoreCase);
    }
    static string NativePath(string path) {
        if (path.StartsWith(@"\\?\UNC\", StringComparison.OrdinalIgnoreCase))
            return @"\\" + path.Substring(8);
        if (path.StartsWith(@"\\?\", StringComparison.OrdinalIgnoreCase) ||
            path.StartsWith(@"\??\", StringComparison.OrdinalIgnoreCase)) return path.Substring(4);
        return path;
    }
    // Extended-length form of a fully qualified, already normalised path, for
    // the raw Win32 calls below. Without the \\?\ prefix CreateFileW refuses a
    // path longer than MAX_PATH (260) with ERROR_PATH_NOT_FOUND unless the host
    // sets LongPathsEnabled AND the process manifest is longPathAware; .NET's
    // own file APIs add the prefix internally, raw P/Invoke does not. Windows
    // does not normalise a \\?\ path, so this accepts only a path that is
    // already normal (callers pass Normal(path)) and refuses '/', '.', '..',
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
    static string FinalPath(SafeFileHandle handle, string held) {
        var buffer = new StringBuilder(32768);
        uint length = GetFinalPathNameByHandleW(handle, buffer, (uint)buffer.Capacity, 0);
        if (length == 0 || length >= buffer.Capacity)
            throw Win32("Nim payload final path unavailable", held, Marshal.GetLastWin32Error(),
                length == 0 ? null : "final path needs " + length + " characters");
        return Normal(NativePath(buffer.ToString()));
    }
    SafeFileHandle Open(string path, bool directory, bool reparse) {
        // OPEN_EXISTING, FILE_FLAG_OPEN_REPARSE_POINT; directories additionally
        // need BACKUP_SEMANTICS. FILE_SHARE_READ alone denies mutation/replacement.
        // Attributes-only opens do not participate in MS-FSA share checks.
        // GENERIC_READ includes FILE_LIST_DIRECTORY/FILE_READ_DATA, so the
        // held directories actually deny later write/delete opens as well.
        // https://learn.microsoft.com/openspecs/windows_protocols/ms-fsa/8c0e3f4f-0729-49f4-a14d-7f7add593819
        var handle = CreateFileW(Win32Path(Normal(path)), 0x80000000u, 1,
            IntPtr.Zero, 3, 0x00200000u | (directory ? 0x02000000u : 0), IntPtr.Zero);
        if (handle.IsInvalid) {
            int error = Marshal.GetLastWin32Error(); handle.Dispose();
            throw Win32("Nim payload hold failed", path, error);
        }
        handles.Add(handle);
        FileInfo info;
        if (!GetFileInformationByHandle(handle, out info))
            throw Win32("Nim payload handle identity unavailable", path, Marshal.GetLastWin32Error());
        if (((info.Attributes & 0x10) != 0) != directory ||
            ((info.Attributes & 0x400) != 0) != reparse)
            throw Refuse("Nim payload object kind differs", path, String.Format(
                "attributes 0x{0:x}, expected {1}, {2}", info.Attributes,
                directory ? "directory" : "file", reparse ? "reparse point" : "no reparse point"));
        return handle;
    }
    // `held` names the junction path; it is used only in diagnostics.
    static string JunctionTarget(SafeFileHandle handle, string held) {
        // FSCTL_GET_REPARSE_POINT and the public MountPointReparseBuffer ABI:
        // https://learn.microsoft.com/windows-hardware/drivers/ddi/ntifs/ns-ntifs-_reparse_data_buffer
        byte[] data = new byte[16384]; uint count;
        if (!DeviceIoControl(handle, 0x000900A8, IntPtr.Zero, 0, data,
            (uint)data.Length, out count, IntPtr.Zero))
            throw Win32("Nim junction query failed", held, Marshal.GetLastWin32Error());
        if (count < 16 || BitConverter.ToUInt32(data, 0) != 0xA0000003 ||
            BitConverter.ToUInt16(data, 4) + 8 != count)
            throw Refuse("selected Nim root is not a directory junction", held, String.Format(
                "{0} reparse bytes, tag 0x{1:x}", count, count < 4 ? 0 : BitConverter.ToUInt32(data, 0)));
        int offset = BitConverter.ToUInt16(data, 8), length = BitConverter.ToUInt16(data, 10);
        if ((offset & 1) != 0 || (length & 1) != 0 || length == 0 || 16 + offset + length > count)
            throw Refuse("malformed selected Nim junction", held);
        return Normal(NativePath(Encoding.Unicode.GetString(data, 16 + offset, length)));
    }
    void Directory(string path) {
        var hold = Open(path, true, false);
        string final = FinalPath(hold, path);
        if (!Same(final, path)) throw Refuse("indirect Nim payload directory", path, "final path " + Show(final));
    }
    void Descendants(string root, string path, string aliasRoot) {
        string relative = Path.GetRelativePath(root, path);
        if (relative == ".") return;
        if (Path.IsPathFullyQualified(relative) || relative == ".." || relative.StartsWith(".." + Path.DirectorySeparatorChar))
            throw Refuse("Nim payload escapes declared prefix", path, "declared prefix " + Show(root));
        string current = root;
        foreach (string part in relative.Split(Path.DirectorySeparatorChar)) {
            current = Path.Combine(current, part); Directory(current);
            if (!Same(root, aliasRoot)) {
                string aliasPart = Path.Combine(aliasRoot, Path.GetRelativePath(root, current));
                var aliasHandle = Open(aliasPart, true, false);
                string final = FinalPath(aliasHandle, aliasPart);
                if (!Same(final, current))
                    throw Refuse("Nim alias descendant target differs", aliasPart,
                        "final path " + Show(final) + ", expected " + Show(current));
            }
        }
    }
    FileStream File(string selected, string expected) {
        var handle = Open(selected, false, false);
        string final = FinalPath(handle, selected);
        if (!Same(final, expected))
            throw Refuse("Nim payload final image differs", selected, "final path " + Show(final) + ", expected " + Show(expected));
        var file = new FileStream(handle, FileAccess.Read); files.Add(file); return file;
    }
    static string Digest(FileStream stream) {
        stream.Position = 0;
        using (var sha = SHA256.Create()) {
            string result = BitConverter.ToString(sha.ComputeHash(stream)).Replace("-", "").ToLowerInvariant();
            stream.Position = 0; return result;
        }
    }
    public static NimPayloadBinding Bind(string selectedPrefix, string declaredPrefix,
        string selectedImage, string selectedInclude) {
        var binding = new NimPayloadBinding();
        try { binding.Initialize(selectedPrefix, declaredPrefix, selectedImage, selectedInclude); return binding; }
        catch { binding.Dispose(); throw; }
    }
    void Initialize(string selectedPrefix, string declaredPrefix, string selectedImage, string selectedInclude) {
        alias = Normal(selectedPrefix); prefix = Normal(declaredPrefix);
        selectedImage = Normal(selectedImage); selectedInclude = Normal(selectedInclude);
        string relativeImage = Path.GetRelativePath(alias, selectedImage);
        string relativeInclude = Path.GetRelativePath(alias, selectedInclude);
        foreach (string relative in new [] { relativeImage, relativeInclude }) {
            if (Path.IsPathFullyQualified(relative) || relative == "." || relative == ".." || relative.StartsWith(".." + Path.DirectorySeparatorChar))
                throw Refuse("Nim metadata escapes selected prefix", relative,
                    "selected prefix " + Show(alias) + ", image " + Show(selectedImage) + ", include " + Show(selectedInclude));
        }
        if (!Same(alias, prefix)) {
            string farm = Path.GetDirectoryName(alias), store = Path.GetDirectoryName(farm);
            if (!String.Equals(Path.GetFileName(farm), "p", StringComparison.OrdinalIgnoreCase) ||
                !System.Text.RegularExpressions.Regex.IsMatch(Path.GetFileName(alias), "^[0-9a-f]{8}$") ||
                !Same(Path.GetDirectoryName(prefix), Path.Combine(store, "prefixes", "nim")))
                throw Refuse("undeclared Nim junction layout", alias, "declared prefix " + Show(prefix));
            // Hold the declared store namespace and its two short internal
            // branches, not all ancestors of the Windows drive. This also
            // prevents replacing p/ or prefixes/nim/ around held leaf roots.
            Directory(store);
            Directory(farm);
            Directory(Path.Combine(store, "prefixes"));
            Directory(Path.Combine(store, "prefixes", "nim"));
            Directory(prefix);
            junction = Open(alias, true, true);
            string target = JunctionTarget(junction, alias);
            if (!Same(target, prefix))
                throw Refuse("foreign Nim junction target", alias, "target " + Show(target) + ", declared prefix " + Show(prefix));
        } else {
            Directory(prefix);
        }
        IncludePath = Normal(Path.Combine(prefix, relativeInclude));
        ImagePath = Normal(Path.Combine(prefix, relativeImage));
        Descendants(prefix, IncludePath, alias);
        Descendants(prefix, Path.GetDirectoryName(ImagePath), alias);
        // The alias root is held no-follow; held physical descendants reject an
        // intermediate reparse, and each selected file's final path must match.
        image = File(selectedImage, ImagePath);
        header = File(Path.Combine(selectedInclude, "nimbase.h"), Path.Combine(IncludePath, "nimbase.h"));
        ImageSha256 = Digest(image); HeaderSha256 = Digest(header);
        Verify();
    }
    public void Verify() {
        if (junction != null) {
            string target = JunctionTarget(junction, alias);
            if (!Same(target, prefix))
                throw Refuse("selected Nim junction changed", alias, "target " + Show(target) + ", declared prefix " + Show(prefix));
        }
        string headerPath = Path.Combine(IncludePath, "nimbase.h");
        if (!Same(FinalPath(image.SafeFileHandle, ImagePath), ImagePath))
            throw Refuse("held Nim payload changed", ImagePath, "final path differs");
        if (!Same(FinalPath(header.SafeFileHandle, headerPath), headerPath))
            throw Refuse("held Nim payload changed", headerPath, "final path differs");
        if (Digest(image) != ImageSha256) throw Refuse("held Nim payload changed", ImagePath, "sha256 differs");
        if (Digest(header) != HeaderSha256) throw Refuse("held Nim payload changed", headerPath, "sha256 differs");
    }
    public void Dispose() {
        for (int i = files.Count - 1; i >= 0; --i) files[i].Dispose();
        for (int i = handles.Count - 1; i >= 0; --i) handles[i].Dispose();
        files.Clear(); handles.Clear();
    }
}
