# Compile `mosaicod` Docker Images

`Dockerfile.mosaicod` builds two images of `mosaicod`, from the repository root:

- the server (the default target), which needs no FFmpeg:

  ```bash
  docker build -f docker/dockerfiles/Dockerfile.mosaicod -t mosaicod:latest .
  ```

- the store optimizer, built with the `video-static` feature, FFmpeg linked in, which turns image
  sequences into videos. It is the same `mosaicod` binary: run it as `mosaicod store-optimizer`.
  It needs nothing of FFmpeg to run; FFmpeg's license, LGPL 2.1 or later, is in
  `/usr/share/licenses/ffmpeg`.

  ```bash
  docker build -f docker/dockerfiles/Dockerfile.mosaicod --target optimizer -t mosaicod-optimizer:latest .
  ```

The store optimizer of the server image optimizes every topic but image sequences, which it
leaves to the optimizer image. Once an image sequence is a video, the server image reads back the
video, whole or as MPEG-DASH segments, but not its images: to read them back, run the server from
the optimizer image too (`mosaicod server`), which has FFmpeg.
