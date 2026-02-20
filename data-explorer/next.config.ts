import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Use Turbopack with empty config (let Next.js handle WASM)
  turbopack: {},

  // Required for DuckDB WASM when using webpack fallback
  webpack: (config, { isServer }) => {
    // DuckDB WASM requires these configurations
    config.experiments = {
      ...config.experiments,
      asyncWebAssembly: true,
    };

    if (!isServer) {
      config.resolve.fallback = {
        ...config.resolve.fallback,
        fs: false,
        net: false,
        tls: false,
      };
    }

    return config;
  },
};

export default nextConfig;
