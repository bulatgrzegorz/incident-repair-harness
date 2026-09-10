FROM docker.io/library/node:22.19.0-bookworm-slim@sha256:4a4884e8a44826194dff92ba316264f392056cbe243dcc9fd3551e71cea02b90 AS opencode
RUN npm install --global opencode-ai@1.17.18

FROM mcr.microsoft.com/dotnet/sdk:10.0.103-noble@sha256:804110c7c824ea14867f4991b411e92f15b1ab6def72f8bb41e08b7da6f3d25e
COPY --from=opencode /usr/local /usr/local
ENV NUGET_PACKAGES=/nuget
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ripgrep \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /cache
COPY Directory.Build.props Directory.Packages.props global.json ./
COPY src/ProductWorker/ProductWorker.csproj src/ProductWorker/packages.lock.json src/ProductWorker/
COPY tests/ProductWorker.Smoke/ProductWorker.Smoke.csproj tests/ProductWorker.Smoke/packages.lock.json tests/ProductWorker.Smoke/
COPY tests/ProductWorker.Tests/ProductWorker.Tests.csproj tests/ProductWorker.Tests/packages.lock.json tests/ProductWorker.Tests/
RUN dotnet restore tests/ProductWorker.Tests/ProductWorker.Tests.csproj --locked-mode \
    && dotnet restore tests/ProductWorker.Smoke/ProductWorker.Smoke.csproj --locked-mode \
    && rm -rf /cache \
    && useradd --create-home --uid 10001 agent \
    && chmod -R a-w /nuget
USER agent
ENV HOME=/home/agent \
    XDG_DATA_HOME=/home/agent/.local/share \
    XDG_CONFIG_HOME=/home/agent/.config \
    XDG_CACHE_HOME=/home/agent/.cache \
    XDG_STATE_HOME=/home/agent/.local/state \
    DOTNET_CLI_TELEMETRY_OPTOUT=1 \
    DOTNET_CLI_WORKLOAD_UPDATE_NOTIFY_DISABLE=true \
    DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1
WORKDIR /workspace
