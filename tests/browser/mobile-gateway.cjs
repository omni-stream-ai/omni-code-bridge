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
  const api = async (path, body) => {
    const response = await context.request.post(config.base + path, {headers: config.headers, data: body});
    assert(response.ok(), await response.text());
    return (await response.json()).data;
  };
  try {
    await api('/control', {acquire: true});
    const {url} = await api('/desktop-ticket', {});
    await page.goto(config.origin + url);
    const iframe = page.locator('#desktop');
    await iframe.waitFor();
    const remote = await (await iframe.elementHandle()).contentFrame();
    await remote.waitForFunction(() => window.client?.connected && Object.keys(client.id_to_window).length > 0, null, {timeout: 30000});
    await page.waitForTimeout(1500);
    const assertFits = async () => {
      const rect = await iframe.boundingBox();
      const viewport = page.viewportSize();
      assert(rect.x >= 0 && rect.y >= 0 && rect.x + rect.width <= viewport.width + 1 && rect.y + rect.height <= viewport.height + 1, JSON.stringify({rect, viewport}));
      assert.deepEqual(await remote.evaluate(() => [innerWidth, innerHeight]), [1440, 900]);
    };
    await assertFits();
    assert.equal(await remote.locator('.simple-keyboard').isVisible(), false);
    await page.setViewportSize({width: 844, height: 390});
    await page.waitForTimeout(400);
    await assertFits();
    await page.setViewportSize({width: 390, height: 844});
    await page.waitForTimeout(400);
    await assertFits();
    await page.locator('#actual').click();
    await page.locator('#pan').click();
    const viewport = await page.locator('#viewport').boundingBox();
    await page.mouse.move(330, viewport.y + 150);
    await page.mouse.down();
    await page.mouse.move(30, viewport.y + 150, {steps: 8});
    await page.mouse.up();
    assert(await page.locator('#viewport').evaluate(el => el.scrollLeft > 0));
    await page.locator('#fit').click();
    await assertFits();
    await page.locator('#keyboard').click();
    assert.equal(await remote.locator('.simple-keyboard').isVisible(), true);
    await page.locator('#keyboard').click();
    assert.equal(await remote.locator('.simple-keyboard').isVisible(), false);
    await page.screenshot({path: config.screenshot});
    // A real touch event traverses browser -> authenticated WS -> Xpra -> X11.
    const rect = await iframe.boundingBox();
    const scale = rect.width / 1440;
    await page.touchscreen.tap(rect.x + 190 * scale, rect.y + 420 * scale);
    await page.waitForTimeout(500);
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);
    await remote.evaluate(() => navigator.clipboard.writeText('中文验收'));
    await page.waitForTimeout(500);
    await page.keyboard.press('Control+v');
    await page.waitForTimeout(1000);
    assert(await remote.evaluate(() => document.hasFocus()));
    await api('/control', {acquire: false});
    const observed = await api('/instructions', {instruction: 'browser eval document.title'});
    assert(JSON.stringify(observed).includes('中文验收'), JSON.stringify(observed));
    // Reusing the old browser cookie after releasing control must be rejected.
    const oldPage = await context.request.get(page.url());
    assert.equal(oldPage.status(), 409);
    assert.deepEqual(errors, []);
    console.log(JSON.stringify({connected: true, fittedDesktop: true, rotation: true, pan: true, keyboardToggle: true, touch: true, chineseInput: true, revoked: true}));
  } finally {
    await browser.close();
  }
})().catch(error => {console.error(error); process.exitCode = 1;});
