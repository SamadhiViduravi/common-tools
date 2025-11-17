// Copyright (c) 2025 WSO2 LLC. (https://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// MUI Import
import { type PaletteMode, alpha } from "@mui/material";

// Color Design Tokens
export const tokens = (mode: PaletteMode) => ({
  ...(mode === "dark"
    ? {
        // Legacy tokens
        grey: {
          100: "#d1d3d4",
          200: "#a8abad",
          300: "#7f8285",
          400: "#5a5d61",
          500: "#444a4e",
          600: "#363b40",
          700: "#2a2d31",
        },
        primary: {
          100: "#d1d3d4",
          200: "#555b5f",
          300: "#373c40",
        },
        secondary: {
          100: "#d1d4d8",
          200: "#52585c",
          300: "#3d4246",
        },
        success: { 100: "#4caf50" },
        warning: { 100: "#a89a63" },
        error: { 100: "#fe4336" },
        gradient: "linear-gradient(to bottom, #363b40, #2a2d31)",
        // Design System Tokens - Dark Mode
        neutral: {
          5: "#0d0d0d",
          10: "#1a1a1a",
          15: "#262626",
          20: "#333333",
          25: "#404040",
          30: "#4d4d4d",
          35: "#595959",
          40: "#666666",
          45: "#737373",
          50: "#808080",
          55: "#8c8c8c",
          60: "#999999",
          65: "#a6a6a6",
          70: "#b2b2b2",
          75: "#bfbfbf",
          80: "#cccccc",
          85: "#d9d9d9",
          90: "#e5e5e5",
          95: "#f2f2f2",
          white: "#ffffff",
          black: "#000000",
        },
        designPrimary: {
          default: "#ff7300",
          95: "#fcf1e8",
          90: "#ffe3cc",
          85: "#ffd5b2",
          80: "#ffc799",
          75: "#ffb980",
          70: "#ffab66",
          65: "#ff9d4d",
          60: "#ff8f33",
          55: "#ff811a",
          45: "#e56800",
          40: "#cc5c00",
          35: "#b25100",
          30: "#994500",
          25: "#803a00",
          20: "#662e00",
          15: "#4d2300",
          10: "#331700",
          5: "#1a0c00",
        },
        designSecondary: {
          default: "#212a30",
          light: "#f3f5f7",
          dark: "#171a1c",
        },
        // Semantic tokens - Dark
        text: {
          100: "#ffffff",
          200: "#ffffffd9",
          300: "#ffffff8f",
        },
        border: {
          light: "#262626",
          medium: "#404040",
          dark: "#4d4d4d",
        },
        background: {
          main: "#1a1a1a",
          mainLight: "#121212",
          dark: "#262626",
          nav: "#212121",
          primary: "#1a1a1a",
          primaryLight: "#331700",
          secondary: "#0d0d0d",
          secondaryLight: "#171a1c",
        },
        navigation: {
          link: "#ffffff8f",
          bg: "rgba(255, 115, 0, 0.2)",
          hover: "#ffffffd9",
          hoverBg: "#ffffff0a",
          clicked: "#ffffff",
          clickedBg: "#ffffff14",
        },
      }
    : {
        // Legacy tokens
        grey: {
          100: "#ffffff",
          200: "#d1d3d4",
          300: "#b1b3b5",
          400: "#949698",
          500: "#777a7c",
          600: "#5a5d61",
          700: "#444a4e",
        },
        primary: {
          100: "#787d81",
          200: "#63696d",
          300: "#444a4e",
        },
        secondary: {
          100: "#868c90",
          200: "#6c7276",
          300: "#52585c",
        },
        success: { 100: "#388e3c" },
        warning: { 100: "#c1ad70" },
        error: { 100: "#fe4336" },
        gradient: "linear-gradient(to bottom, #f1f2f3, #d1d3d4)",
        // Design System Tokens - Light Mode
        neutral: {
          5: "#0d0d0d",
          10: "#1a1a1a",
          15: "#262626",
          20: "#333333",
          25: "#404040",
          30: "#4d4d4d",
          35: "#595959",
          40: "#666666",
          45: "#737373",
          50: "#808080",
          55: "#8c8c8c",
          60: "#999999",
          65: "#a6a6a6",
          70: "#b2b2b2",
          75: "#bfbfbf",
          80: "#cccccc",
          85: "#d9d9d9",
          90: "#e5e5e5",
          95: "#f2f2f2",
          white: "#ffffff",
          black: "#000000",
        },
        designPrimary: {
          default: "#ff7300",
          95: "#fcf1e8",
          90: "#ffe3cc",
          85: "#ffd5b2",
          80: "#ffc799",
          75: "#ffb980",
          70: "#ffab66",
          65: "#ff9d4d",
          60: "#ff8f33",
          55: "#ff811a",
          45: "#e56800",
          40: "#cc5c00",
          35: "#b25100",
          30: "#994500",
          25: "#803a00",
          20: "#662e00",
          15: "#4d2300",
          10: "#331700",
          5: "#1a0c00",
        },
        designSecondary: {
          default: "#212a30",
          light: "#f3f5f7",
          dark: "#171a1c",
        },
        // Semantic tokens - Light
        text: {
          100: "#000000",
          200: "#000000cc",
          300: "#0000008f",
        },
        border: {
          light: "#e5e5e5",
          medium: "#cccccc",
          dark: "#d9d9d9",
        },
        background: {
          main: "#ffffff",
          mainLight: "#f7f7f7",
          dark: undefined,
          nav: undefined,
          primary: "#ffffff",
          primaryLight: "#ffe3cc",
          secondary: "#212a30",
          secondaryLight: "#f3f5f7",
        },
        navigation: {
          link: "#ffffff8f",
          bg: "rgba(255, 115, 0, 0.2)",
          hover: "#ffffffd9",
          hoverBg: "#ffffff0a",
          clicked: "#ffffff",
          clickedBg: "#ffffff14",
        },
      }),
});

// Extend background and custom palette types
declare module "@mui/material/styles" {
  interface TypeBackground {
    autocomplete?: string;
    dataGrid?: string;
    layout?: string;
    gradient?: string;
    main?: string;
    mainLight?: string;
    dark?: string;
    nav?: string;
    primary?: string;
    primaryLight?: string;
    secondary?: string;
    secondaryLight?: string;
  }

  interface Palette {
    neutral: Record<string | number, string | undefined>;
    designPrimary: Record<string | number, string>;
    designSecondary: Record<string, string>;
    textTokens: Record<number, string>;
    border: Record<string, string>;
    navigation: Record<string, string>;
  }

  interface PaletteOptions {
    background?: Partial<TypeBackground>;
    neutral?: Record<string | number, string | undefined>;
    designPrimary?: Record<string | number, string>;
    designSecondary?: Record<string, string>;
    textTokens?: Record<number, string>;
    border?: Record<string, string>;
    navigation?: Record<string, string>;
  }
}

// MUI Theme Settings
export const themeSettings = (mode: PaletteMode) => {
  const colors = tokens(mode);

  return {
    palette: {
      mode: mode,
      ...(mode === "dark"
        ? {
            primary: {
              main: colors.primary[100],
              dark: colors.primary[300],
            },
            secondary: {
              main: colors.grey[200],
              dark: colors.secondary[200],
            },
            success: { main: colors.success[100] },
            warning: { main: colors.warning[100] },
            error: { main: colors.error[100] },
            background: {
              default: colors.grey[700],
              form: colors.grey[600],
              banner: colors.primary[200],
              autocomplete: colors.grey[400],
              dataGrid: colors.grey[500],
              layout: colors.grey[100],
              gradient: colors.gradient,
              main: colors.background.main,
              mainLight: colors.background.mainLight,
              dark: colors.background.dark,
              nav: colors.background.nav,
              primary: colors.background.primary,
              primaryLight: colors.background.primaryLight,
              secondary: colors.background.secondary,
              secondaryLight: colors.background.secondaryLight,
            },
            neutral: colors.neutral,
            designPrimary: colors.designPrimary,
            designSecondary: colors.designSecondary,
            textTokens: colors.text,
            border: colors.border,
            navigation: colors.navigation,
          }
        : {
            primary: {
              main: colors.primary[300],
              dark: colors.primary[300],
            },
            secondary: {
              main: colors.secondary[200],
              dark: colors.secondary[300],
            },
            success: { main: colors.success[100] },
            warning: { main: colors.warning[100] },
            error: { main: colors.error[100] },
            background: {
              default: colors.grey[100],
              form: colors.grey[100],
              banner: colors.primary[200],
              autocomplete: colors.grey[400],
              dataGrid: colors.grey[300],
              layout: colors.grey[100],
              gradient: colors.gradient,
              main: colors.background.main,
              mainLight: colors.background.mainLight,
              dark: colors.background.dark,
              nav: colors.background.nav,
              primary: colors.background.primary,
              primaryLight: colors.background.primaryLight,
              secondary: colors.background.secondary,
              secondaryLight: colors.background.secondaryLight,
            },
            neutral: colors.neutral,
            designPrimary: colors.designPrimary,
            designSecondary: colors.designSecondary,
            textTokens: colors.text,
            border: colors.border,
            navigation: colors.navigation,
          }),
    },
    typography: {
      fontSize: 11,
      fontFamily: ["Poppins"].join(","),
      h1: { fontSize: 38, fontWeight: 700 },
      h2: { fontSize: 32, fontWeight: 600 },
      h3: { fontSize: 24, fontWeight: 500 },
      h4: { fontSize: 20 },
      h5: { fontSize: 16 },
      h6: { fontSize: 14 },
    },
    components: {
      MuiDataGrid: {
        styleOverrides: {
          columnHeader: {
            backgroundColor:
              mode === "dark"
                ? alpha(colors.primary[300], 0.9)
                : alpha(colors.primary[300], 0.2),
            fontWeight: 1000,
          },
          columnHeaderTitle: {
            fontWeight: 1000,
          },
        },
      },
      MuiButton: {
        styleOverrides: {
          contained: {
            backgroundColor: "#ff7300",
            "&:hover": { backgroundColor: "#e76000" },
            fontWeight: "bold",
            letterSpacing: "2px",
            padding: "9px 12px",
            borderRadius: "8px",
            color: "white",
          },
        },
      },
      MuiCssBaseline: {
        styleOverrides:
          mode === "dark"
            ? `
            input:-webkit-autofill,
            input:-webkit-autofill:hover,
            input:-webkit-autofill:focus,
            input:-webkit-autofill:active {
              -webkit-box-shadow: 0 0 0 30px ${colors.grey[700]} inset !important;
            }`
            : `
            input:-webkit-autofill,
            input:-webkit-autofill:hover,
            input:-webkit-autofill:focus,
            input:-webkit-autofill:active {
              -webkit-box-shadow: 0 0 0 30px ${colors.grey[100]} inset !important;
            }`,
      },
    },
    breakpoints: {
      values: {
        xs: 0,
        sm: 600,
        md: 960,
        lg: 1280,
        xl: 1920,
      },
    },
  };
};
