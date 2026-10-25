ARG BASE_IMAGE=archlinux:base
FROM ${BASE_IMAGE} AS runtime

RUN pacman-key --init && pacman-key --populate archlinux \
    && pacman -Syu --noconfirm --needed \
      kwin xdg-desktop-portal xdg-desktop-portal-kde pipewire wireplumber \
      mesa dbus gtk4 python-gobject python-cairo at-spi2-core python-pillow \
      qt6-wayland qt6-tools xorg-xwayland \
    && pacman -Scc --noconfirm \
    && useradd --create-home --uid 1000 tester \
    && setcap -r /usr/bin/kwin_wayland

COPY launch.sh session.sh fixture.py ui_inspect.py /opt/wayland-test/
USER tester
ENV HOME=/home/tester \
    XDG_RUNTIME_DIR=/tmp/wayland-runtime \
    XDG_SESSION_TYPE=wayland \
    XDG_CURRENT_DESKTOP=KDE \
    LIBGL_ALWAYS_SOFTWARE=1 \
    GALLIUM_DRIVER=llvmpipe \
    QT_QUICK_BACKEND=software \
    QT_LINUX_ACCESSIBILITY_ALWAYS_ON=1 \
    NO_AT_BRIDGE=0
ENTRYPOINT ["bash", "/opt/wayland-test/launch.sh"]

FROM runtime AS builder
USER root
RUN pacman -S --noconfirm --needed rust clang gcc pkgconf && pacman -Scc --noconfirm
USER tester
ENTRYPOINT ["bash"]

FROM runtime AS final
