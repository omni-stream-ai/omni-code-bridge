const fs = require('node:fs');
const assert = require('node:assert/strict');
const {chromium} = require('playwright');

(async () => {
  const config = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
  const browser = await chromium.launch({
    executablePath: process.env.X11BOX_BROWSER_EXECUTABLE || undefined,
    headless: true, args: ['--no-sandbox'],
  });
  const context = await browser.newContext({viewport: {width: 390, height: 844},
    isMobile: true, hasTouch: true, locale: 'zh-CN'});
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  page.on('console', message => { if (message.type() === 'error') console.error(message.text()); });
  await page.addInitScript(() => {
    window.__connections = 0;
    document.addEventListener('connection-established', () => window.__connections++);
  });
  const api = async (path, body) => {
    const response = await context.request.post(config.base + path, {headers: config.headers, data: body});
    assert(response.ok(), await response.text());
    return (await response.json()).data;
  };
  try {
    await api('/control', {acquire: true});
    const {url} = await api('/desktop-ticket', {});
    await page.goto(config.origin + url);
    await page.waitForFunction(() => window.__connections > 0, null, {timeout: 30000});
    await page.waitForTimeout(1500);
    await page.screenshot({path: config.screenshot});
    // A real touch event traverses browser -> authenticated WS -> Xpra -> X11.
    await page.touchscreen.tap(190, 420);
    await page.waitForTimeout(500);
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);
    await page.evaluate(() => navigator.clipboard.writeText('中文验收'));
    await page.waitForTimeout(500);
    await page.keyboard.press('Control+v');
    await page.waitForTimeout(1000);
    await api('/control', {acquire: false});
    const observed = await api('/instructions', {instruction: 'browser eval document.title'});
    assert(JSON.stringify(observed).includes('中文验收'), JSON.stringify(observed));
    // Reusing the old browser cookie after releasing control must be rejected.
    const oldPage = await context.request.get(page.url());
    assert.equal(oldPage.status(), 409);
    assert.deepEqual(errors, []);
    console.log(JSON.stringify({connected: true, touch: true, chineseInput: true, revoked: true}));
  } finally {
    await browser.close();
  }
})().catch(error => {console.error(error); process.exitCode = 1;});
