// 仅识别本项目生成的日期目录及哈希文件名，不猜测第三方图片服务的路由。
const MANAGED_IMAGE_PATH = /^(.*)\/images\/(\d{4}\/\d{2}\/\d{2}\/\d{10,}_[a-f0-9]{32}\.png)$/i;

function managedImageParts(src: string) {
  try {
    const url = new URL(src, "https://managed-image.invalid");
    if (!/^https?:$/.test(url.protocol)) return null;
    const match = url.pathname.match(MANAGED_IMAGE_PATH);
    return match ? { prefix: match[1], path: match[2], search: url.search } : null;
  } catch { return null; }
}

export function getManagedImageUrl(src: string, baseUrl = "") {
  const parts = managedImageParts(src);
  if (!parts) return src;
  return `${baseUrl.replace(/\/$/, "")}${parts.prefix}/images/${parts.path}${parts.search}`;
}

export function getManagedThumbnailUrl(src: string, baseUrl = "") {
  const parts = managedImageParts(src);
  if (!parts) return src;
  return `${baseUrl.replace(/\/$/, "")}${parts.prefix}/image-thumbnails/${parts.path}${parts.search}`;
}
