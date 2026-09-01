declare module "*.tf" { const content: string; export default content; }
declare module "*.yml" { const content: string; export default content; }
declare module "*.cfg" { const content: string; export default content; }
declare module "*.ini" { const content: string; export default content; }
declare module "*.sh" { const content: string; export default content; }
declare module "*.py" { const content: string; export default content; }
declare module "*.json" { const content: string; export default content; }
declare module "*.properties" { const content: string; export default content; }
declare module "*.service" { const content: string; export default content; }
declare module "*.timer" { const content: string; export default content; }
// ONCE's own templates, reached through the shared ssh module: extensionless
// names need their own declarations.
declare module "*/authorized-keys" { const content: string; export default content; }
declare module "*/deploy" { const content: string; export default content; }
declare module "*/once" { const content: string; export default content; }
