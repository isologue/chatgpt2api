"use client";

import { useEffect, useMemo, useState } from "react";

import { cn } from "@/lib/utils";
import { ImageOff } from "lucide-react";
import { getManagedImageUrl, getManagedThumbnailUrl } from "@/lib/image-urls";
import webConfig from "@/constants/common-env";

type ImageThumbnailProps = {
  src: string;
  thumbnailSrc?: string;
  alt?: string;
  className?: string;
  imageClassName?: string;
};

export function getImageThumbnailUrl(src: string) {
  return getManagedThumbnailUrl(src, webConfig.apiUrl);
}

export function ImageThumbnail({ src: rawSrc, thumbnailSrc, alt = "", className, imageClassName }: ImageThumbnailProps) {
  const src = getImagePreviewUrl(rawSrc);
  const initialSrc = useMemo(() => thumbnailSrc || getImageThumbnailUrl(rawSrc), [rawSrc, thumbnailSrc]);
  const [failed, setFailed] = useState(false);
  const [currentSrc, setCurrentSrc] = useState(initialSrc);

  useEffect(() => {
    setCurrentSrc(initialSrc);
    setFailed(false);
  }, [initialSrc]);

  return (
    <span className={cn("block overflow-hidden bg-stone-100", className)}>
      {failed ? <ImageOff aria-label="图片不可用" className="m-auto size-4 text-stone-400" /> : <img
        src={currentSrc}
        alt={alt}
        className={cn("h-full w-full object-cover", imageClassName)}
        loading="lazy"
        decoding="async"
        onError={() => {
          if (currentSrc !== src) {
            setCurrentSrc(src);
          } else {
            setFailed(true);
          }
        }}
      />}
    </span>
  );
}

export function getImagePreviewUrl(src: string) {
  return getManagedImageUrl(src, webConfig.apiUrl);
}
