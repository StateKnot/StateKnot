// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

export const locales = ["en", "zh-CN"] as const;
export type Locale = (typeof locales)[number];

export const defaultLocale: Locale = "en";

export const localeFromPath = (path: string): Locale =>
  path === "/zh" || path.startsWith("/zh/") ? "zh-CN" : "en";

export const stripLocaleFromPath = (path: string): string => {
  if (path === "/zh" || path === "/zh/") return "/";
  if (path.startsWith("/zh/")) return path.slice(3) || "/";
  return path;
};

export const localizePath = (path: string, locale: Locale): string => {
  if (!path.startsWith("/") || path.startsWith("//")) return path;

  const unlocalized = stripLocaleFromPath(path);
  if (locale === "en") return unlocalized;
  return unlocalized === "/" ? "/zh/" : `/zh${unlocalized}`;
};

export const alternateLocale = (locale: Locale): Locale =>
  locale === "en" ? "zh-CN" : "en";

export const alternateLocalePath = (path: string): string =>
  localizePath(path, alternateLocale(localeFromPath(path)));

export const languageName = (locale: Locale): string =>
  locale === "en" ? "English" : "中文";

export const alternateLanguageLabel = (locale: Locale): string =>
  locale === "en" ? "中文" : "English";
