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

    static string Normal(string path) {
        if (String.IsNullOrEmpty(path) || !Path.IsPathFullyQualified(path))
            throw new InvalidDataException("missing absolute Nim payload role");
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
    static string FinalPath(SafeFileHandle handle) {
        var buffer = new StringBuilder(32768);
        uint length = GetFinalPathNameByHandleW(handle, buffer, (uint)buffer.Capacity, 0);
        if (length == 0 || length >= buffer.Capacity)
            throw new Win32Exception(Marshal.GetLastWin32Error(), "Nim payload final path unavailable");
        return Normal(NativePath(buffer.ToString()));
    }
    SafeFileHandle Open(string path, bool directory, bool reparse) {
        // OPEN_EXISTING, FILE_FLAG_OPEN_REPARSE_POINT; directories additionally
        // need BACKUP_SEMANTICS. FILE_SHARE_READ alone denies mutation/replacement.
        var handle = CreateFileW(path, directory ? 0x80u : 0x80000000u, 1,
            IntPtr.Zero, 3, 0x00200000u | (directory ? 0x02000000u : 0), IntPtr.Zero);
        if (handle.IsInvalid) {
            int error = Marshal.GetLastWin32Error(); handle.Dispose();
            throw new Win32Exception(error, "Nim payload hold failed");
        }
        handles.Add(handle);
        FileInfo info;
        if (!GetFileInformationByHandle(handle, out info))
            throw new Win32Exception(Marshal.GetLastWin32Error(), "Nim payload handle identity unavailable");
        if (((info.Attributes & 0x10) != 0) != directory ||
            ((info.Attributes & 0x400) != 0) != reparse)
            throw new InvalidDataException("Nim payload object kind differs");
        return handle;
    }
    static string JunctionTarget(SafeFileHandle handle) {
        // FSCTL_GET_REPARSE_POINT and the public MountPointReparseBuffer ABI:
        // https://learn.microsoft.com/windows-hardware/drivers/ddi/ntifs/ns-ntifs-_reparse_data_buffer
        byte[] data = new byte[16384]; uint count;
        if (!DeviceIoControl(handle, 0x000900A8, IntPtr.Zero, 0, data,
            (uint)data.Length, out count, IntPtr.Zero))
            throw new Win32Exception(Marshal.GetLastWin32Error(), "Nim junction query failed");
        if (count < 16 || BitConverter.ToUInt32(data, 0) != 0xA0000003 ||
            BitConverter.ToUInt16(data, 4) + 8 != count)
            throw new InvalidDataException("selected Nim root is not a directory junction");
        int offset = BitConverter.ToUInt16(data, 8), length = BitConverter.ToUInt16(data, 10);
        if ((offset & 1) != 0 || (length & 1) != 0 || length == 0 || 16 + offset + length > count)
            throw new InvalidDataException("malformed selected Nim junction");
        return Normal(NativePath(Encoding.Unicode.GetString(data, 16 + offset, length)));
    }
    void Directory(string path) {
        var hold = Open(path, true, false);
        if (!Same(FinalPath(hold), path)) throw new InvalidDataException("indirect Nim payload directory");
    }
    void Descendants(string root, string path, string aliasRoot) {
        string relative = Path.GetRelativePath(root, path);
        if (relative == ".") return;
        if (Path.IsPathFullyQualified(relative) || relative == ".." || relative.StartsWith(".." + Path.DirectorySeparatorChar))
            throw new InvalidDataException("Nim payload escapes declared prefix");
        string current = root;
        foreach (string part in relative.Split(Path.DirectorySeparatorChar)) {
            current = Path.Combine(current, part); Directory(current);
            if (!Same(root, aliasRoot)) {
                string aliasPart = Path.Combine(aliasRoot, Path.GetRelativePath(root, current));
                var aliasHandle = Open(aliasPart, true, false);
                if (!Same(FinalPath(aliasHandle), current))
                    throw new InvalidDataException("Nim alias descendant target differs");
            }
        }
    }
    FileStream File(string selected, string expected) {
        var handle = Open(selected, false, false);
        if (!Same(FinalPath(handle), expected)) throw new InvalidDataException("Nim payload final image differs");
        var file = new FileStream(handle, FileAccess.Read); files.Add(file); return file;
    }
    static string Digest(FileStream stream) {
        stream.Position = 0;
        using (var sha = SHA256.Create()) {
            string result = BitConverter.ToString(sha.ComputeHash(stream)).Replace("-", "");
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
                throw new InvalidDataException("Nim metadata escapes selected prefix");
        }
        if (!Same(alias, prefix)) {
            string farm = Path.GetDirectoryName(alias), store = Path.GetDirectoryName(farm);
            if (!String.Equals(Path.GetFileName(farm), "p", StringComparison.OrdinalIgnoreCase) ||
                !System.Text.RegularExpressions.Regex.IsMatch(Path.GetFileName(alias), "^[0-9a-f]{8}$") ||
                !Same(Path.GetDirectoryName(prefix), Path.Combine(store, "prefixes", "nim")))
                throw new InvalidDataException("undeclared Nim junction layout");
            // Hold the declared store namespace and its two short internal
            // branches, not all ancestors of the Windows drive. This also
            // prevents replacing p/ or prefixes/nim/ around held leaf roots.
            Directory(store);
            Directory(farm);
            Directory(Path.Combine(store, "prefixes"));
            Directory(Path.Combine(store, "prefixes", "nim"));
            Directory(prefix);
            junction = Open(alias, true, true);
            if (!Same(JunctionTarget(junction), prefix)) throw new InvalidDataException("foreign Nim junction target");
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
        if (junction != null && !Same(JunctionTarget(junction), prefix))
            throw new InvalidDataException("selected Nim junction changed");
        if (!Same(FinalPath(image.SafeFileHandle), ImagePath) ||
            !Same(FinalPath(header.SafeFileHandle), Path.Combine(IncludePath, "nimbase.h")) ||
            Digest(image) != ImageSha256 || Digest(header) != HeaderSha256)
            throw new InvalidDataException("held Nim payload changed");
    }
    public void Dispose() {
        for (int i = files.Count - 1; i >= 0; --i) files[i].Dispose();
        for (int i = handles.Count - 1; i >= 0; --i) handles[i].Dispose();
        files.Clear(); handles.Clear();
    }
}
