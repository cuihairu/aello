import { defineConfig } from 'vitepress'

const base = '/aello/'

// First-visit language auto-redirect: on the first page load of a browser
// session, if the user's browser language is Chinese and they landed on an
// English page, send them to the matching /zh/ page. The decision is recorded
// in sessionStorage so the manual language switcher always wins afterwards
// (a switch to English is never bounced back, since the flag is already set).
function langRedirectHead() {
  return [
    [
      'script',
      {},
    `
(function () {
  try {
    if (sessionStorage.getItem('aello:docs:lang')) return;
    var path = window.location.pathname;
    if (path.indexOf(${JSON.stringify(base)}) !== 0) return; // not on this site
    var zhRoot = ${JSON.stringify(base)} + 'zh';
    if (path === zhRoot || path.indexOf(zhRoot + '/') === 0) {
      sessionStorage.setItem('aello:docs:lang', 'done');
      return;
    }
    var lang = (navigator.languages && navigator.languages[0]) || navigator.language || '';
    if (!/^zh/i.test(lang)) {
      sessionStorage.setItem('aello:docs:lang', 'done');
      return;
    }
    sessionStorage.setItem('aello:docs:lang', 'zh');
    // base ends in '/'; slice from base.length - 1 to keep the leading '/'
    var rest = path.slice(${JSON.stringify(base)}.length - 1);
    if (rest === '') rest = '/';
    window.location.replace(zhRoot + rest + window.location.search + window.location.hash);
  } catch (e) {}
})();
`.trim(),
    ],
  ]
}

export default defineConfig({
  base,
  title: 'Aello',
  description: 'Messaging runtime for game servers',
  cleanUrls: true,
  ignoreDeadLinks: false,
  head: langRedirectHead(),
  themeConfig: {
    outline: { level: [2, 3] },
    socialLinks: [{ icon: 'github', link: 'https://github.com/cuihairu/aello' }],
  },
  locales: {
    root: {
      label: 'English',
      lang: 'en-US',
      title: 'Aello',
      description: 'Messaging runtime for game servers',
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
                { text: 'Source layer analysis', link: '/design/source-layer-analysis-2026-11-07' },
                { text: 'Source glossary', link: '/design/source-glossary-2026-11-07' },
                { text: 'Reliability proposal', link: '/design/reliability-at-least-once' },
                { text: 'TLS proposal', link: '/design/tls-mtls' },
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
      description: '面向游戏服务器的消息运行时',
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
                { text: '源码分层分析', link: '/zh/design/source-layer-analysis-2026-11-07' },
                { text: '源码术语表', link: '/zh/design/source-glossary-2026-11-07' },
                { text: '可靠性送达提案', link: '/zh/design/reliability-at-least-once' },
                { text: 'TLS 提案', link: '/zh/design/tls-mtls' },
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
