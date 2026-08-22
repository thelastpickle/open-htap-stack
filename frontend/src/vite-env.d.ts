// Declares the modules Vite resolves and TypeScript does not: a CSS import, an
// asset import, and import.meta.env.  TypeScript 7 rejects a side-effect import
// of a file it has no declaration for, where 5.x ignored it, so the four CSS
// imports in main.tsx and pages/Map.tsx need this reference to compile.
/// <reference types="vite/client" />
