{ pkgs }: {
  deps = [
    pkgs.grpc-tools
    pkgs.python310Full
    # Các gói khác mà bạn muốn thêm
    pkgs.cowsay
    pkgs.glibcLocales
  ];
}
