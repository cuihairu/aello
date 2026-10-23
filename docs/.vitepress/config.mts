import { defineConfig } from 'vitepress'

export default defineConfig({
  base: '/aello/',
  title: 'Aello',
  description: 'High-performance messaging runtime for game servers',
  cleanUrls: true,
  ignoreDeadLinks: false,
  themeConfig: {
    outline: { level: [2, 3] },
    socialLinks: [{ icon: 'github', link: 'https://github.com/cuihairu/aello' }],
  },
  locales: {
    root: {
      label: 'English',
      lang: 'en-US',
      title: 'Aello',
      description: 'High-performance messaging runtime for game servers',
      themeConfig: {
        siteTitle: 'Aello',
        nav: [
          { text: 'Home', link: '/' },
          { text: 'Design', link: '/design/aello-adoption-2026-10-16', activeMatch: '/design/' },
        ],
        sidebar: {
          '/design/': [
            {
              text: 'Design',
              items: [
                { text: 'Adoption decisions', link: '/design/aello-adoption-2026-10-16' },
                { text: 'External analysis', link: '/design/aello-analysis-2026-10-16' },
                { text: 'File-by-file audit', link: '/design/aello-audit-2026-10-16' },
              ],
            },
          ],
        },
      },
    },
    zh: {
      label: '简体中文',
      lang: 'zh-CN',
      link: '/zh/',
      title: 'Aello',
      description: '面向游戏服务器的高性能消息运行时',
      themeConfig: {
        siteTitle: 'Aello',
        nav: [
          { text: '首页', link: '/zh/' },
          { text: '设计', link: '/zh/design/aello-adoption-2026-10-16', activeMatch: '/zh/design/' },
        ],
        sidebar: {
          '/zh/design/': [
            {
              text: '设计文档',
              items: [
                { text: '分析稿吸收结论', link: '/zh/design/aello-adoption-2026-10-16' },
                { text: '外部分析报告', link: '/zh/design/aello-analysis-2026-10-16' },
                { text: '逐文件审计', link: '/zh/design/aello-audit-2026-10-16' },
              ],
            },
          ],
        },
        outline: { level: [2, 3], label: '本页目录' },
        docFooter: { prev: '上一篇', next: '下一篇' },
        darkModeSwitchLabel: '主题',
        sidebarMenuLabel: '菜单',
        returnToTopLabel: '回到顶部',
        lastUpdated: { text: '最后更新于' },
      },
    },
  },
})
