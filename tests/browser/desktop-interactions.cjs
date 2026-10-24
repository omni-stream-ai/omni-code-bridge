// Standalone mobile interaction regression; no desktop service required.
const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const assert = require('node:assert/strict');
const {chromium} = require('playwright');

(async () => {
  const html = fs.readFileSync(path.join(__dirname, '../../plugins/x11box-acceptance/desktop-viewer.html'), 'utf8')
    .replace('__DESKTOP_WIDTH__', '1440').replace('__DESKTOP_HEIGHT__', '900');
  const server = http.createServer((req, res) => {
    res.setHeader('Content-Type', 'text/html');
    res.end(req.url.startsWith('/index.html') ? `<!doctype html><style>html,body{margin:0;width:100%;height:100%}</style>
      <script>window.packets=[];window.wheels=[];window.client={connected:true,focused_wid:7,send:p=>packets.push(p)};
      document.addEventListener('wheel',e=>wheels.push([e.clientX,e.clientY,e.deltaX,e.deltaY]));</script>` : html);
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const browser = await chromium.launch({executablePath: process.env.X11BOX_BROWSER_EXECUTABLE || undefined, headless: true, args: ['--no-sandbox']});
  try {
    const page = await browser.newPage({viewport: {width: 390, height: 844}, isMobile: true, hasTouch: true, locale: 'zh-CN'});
    const errors = [];
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(`http://127.0.0.1:${server.address().port}/viewer`);
    const remote = await (await page.locator('#desktop').elementHandle()).contentFrame();
    await remote.waitForFunction(() => window.client?.connected);
    await page.locator('#keyboard').click();
    assert(await page.locator('#native-input').evaluate(el => document.activeElement === el));
    await page.locator('#native-input').evaluate(el => {
      el.dispatchEvent(new CompositionEvent('compositionstart'));
      el.value = '中文';
      el.dispatchEvent(new InputEvent('input', {isComposing:true}));
    });
    assert.equal(await remote.evaluate(() => packets.length), 0);
    await page.locator('#native-input').evaluate(el => {
      el.dispatchEvent(new CompositionEvent('compositionend'));
      el.dispatchEvent(new InputEvent('input'));
    });
    assert.deepEqual(await remote.evaluate(() => packets.map(p => [p[2],p[3],p[6]])),
      [['U4E2D',true,'中'],['U4E2D',false,'中'],['U6587',true,'文'],['U6587',false,'文']]);
    await page.keyboard.press('Backspace');
    await page.keyboard.press('Enter');
    await page.locator('#native-input').evaluate(el => el.dispatchEvent(new InputEvent('input', {inputType:'deleteContentBackward'})));
    assert.deepEqual(await remote.evaluate(() => packets.slice(4).map(p => p[2])), ['BackSpace','BackSpace','Return','Return','BackSpace','BackSpace']);
    await page.locator('#keyboard').click();
    assert.equal(await page.locator('#native-input').isVisible(), false);
    await page.locator('#scroll').click();
    const box = await page.locator('#desktop').boundingBox();
    await page.mouse.move(box.x + 150, box.y + 160);
    await page.mouse.down();
    await page.mouse.move(box.x + 150, box.y + 60, {steps:4});
    await page.mouse.up();
    assert(await remote.evaluate(() => wheels.some(w => w[3] > 0)));
    assert.equal(await page.locator('#viewport').evaluate(el => el.scrollTop), 0);
    await page.locator('#actual').click();
    await page.locator('#pan').click();
    await page.mouse.move(330, box.y + 100);
    await page.mouse.down();
    await page.mouse.move(30, box.y + 100, {steps:4});
    await page.mouse.up();
    assert(await page.locator('#viewport').evaluate(el => el.scrollLeft > 0));
    assert.equal(await page.locator('#scroll').getAttribute('aria-pressed'), 'false');
    await page.locator('#fit').click();
    await page.setViewportSize({width:844,height:390});
    await page.waitForTimeout(100);
    const rect = await page.locator('#desktop').boundingBox();
    assert(rect.x + rect.width <= 845 && rect.y + rect.height <= 391);
    assert.deepEqual(errors, []);
    console.log('PASS: native input focus, Chinese IME commits once, deletion/enter, remote scroll, local pan, landscape fit');
  } finally {
    await browser.close();
    await new Promise(resolve => server.close(resolve));
  }
})().catch(e => {console.error(e);process.exitCode=1;});
