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
    assert.deepEqual(await page.locator('nav button').allTextContents(), ['适应屏幕', '手机键盘']);
    const gestures = await context.newCDPSession(page);
    const viewport = await page.locator('#viewport').boundingBox();
    const pair = spread => [{x:195-spread,y:viewport.y+120},{x:195+spread,y:viewport.y+120}];
    await gestures.send('Input.dispatchTouchEvent', {type:'touchStart',touchPoints:pair(35)});
    await gestures.send('Input.dispatchTouchEvent', {type:'touchMove',touchPoints:pair(110)});
    await gestures.send('Input.dispatchTouchEvent', {type:'touchEnd',touchPoints:[]});
    await gestures.send('Input.dispatchTouchEvent', {type:'touchStart',touchPoints:[{x:330,y:viewport.y+150}]});
    await gestures.send('Input.dispatchTouchEvent', {type:'touchMove',touchPoints:[{x:30,y:viewport.y+150}]});
    await gestures.send('Input.dispatchTouchEvent', {type:'touchEnd',touchPoints:[]});
    await gestures.detach();
    assert(await page.locator('#viewport').evaluate(el => el.scrollLeft > 0));
    await page.locator('#fit').click();
    await assertFits();
    const focusSignal = async () => page.evaluate(async () => (await (await fetch('input-focus')).json()).focused);
    const tapRemote = async (x, y) => {
      const rect = await iframe.boundingBox();
      const scale = rect.width / 1440;
      await page.touchscreen.tap(rect.x + x * scale, rect.y + y * scale);
    };
    assert.equal(await focusSignal(), false, 'initial button focus is not editable');
    await tapRemote(190, 420);
    await page.waitForFunction(() => document.querySelector('#native-input').classList.contains('active'));
    assert.equal(await focusSignal(), true, 'real remote editor focus opens the keyboard automatically');
    await tapRemote(60, config.native ? 50 : 100);
    await page.waitForFunction(() => !document.querySelector('#native-input').classList.contains('active'));
    assert.equal(await focusSignal(), false, 'remote button focus closes the automatic keyboard');
    await page.locator('#keyboard').click();
    assert(await page.locator('#native-input').evaluate(el => document.activeElement === el));
    await page.evaluate(() => {
      window.inputTrace = [];
      const input = document.querySelector('#native-input');
      for (const type of ['input', 'paste']) input.addEventListener(type, event => {
        inputTrace.push({type, inputType: event.inputType, data: event.data, pasted: event.clipboardData?.getData('text/plain'), value: input.value});
      });
    });
    await page.locator('#keyboard').click();
    assert.equal(await page.locator('#native-input').isVisible(), false);
    await page.screenshot({path: config.screenshot});
    // Allow the focus poll to observe the button after manual dismissal.
    await tapRemote(60, config.native ? 50 : 100);
    await page.waitForTimeout(700);
    // A real touch event traverses browser -> authenticated WS -> Xpra -> X11.
    await tapRemote(190, 420);
    await page.waitForFunction(() => document.querySelector('#native-input').classList.contains('active'));
    assert(await page.locator('#native-input').evaluate(el => document.activeElement === el));
    if (config.webkit) {
      // This fixture checks the cross-process focus bridge and resulting input.
      await page.keyboard.insertText('WebKit focus verification');
      await page.waitForTimeout(500);
      await api('/control', {acquire: false});
      const observed = await api('/instructions', {instruction: 'observe'});
      assert(JSON.stringify(observed).includes('WebKit focus verification'), JSON.stringify(observed));
      assert.equal((await context.request.get(page.url())).status(), 409);
      assert.deepEqual(errors, []);
      console.log(JSON.stringify({webkit: true, automaticKeyboard: true, remoteButtonClosesKeyboard: true, remoteTextInput: true, revoked: true}));
      return;
    }
    await page.keyboard.insertText('discard');
    await page.keyboard.press('Control+a');
    await page.keyboard.insertText('hallo!');
    await page.keyboard.press('Shift+ArrowLeft');
    await page.keyboard.press('Backspace');
    await page.keyboard.press('Home');
    await page.keyboard.press('ArrowRight');
    await page.keyboard.press('Delete');
    await page.keyboard.insertText('e');
    await page.keyboard.press('End');
    const cdp = await context.newCDPSession(page);
    await cdp.send('Input.imeSetComposition', {text:'中文验收', selectionStart:4, selectionEnd:4});
    await cdp.send('Input.insertText', {text:'中文验收'});
    await page.keyboard.press('Backspace');
    await page.keyboard.insertText('通过🌍!');
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);
    await page.evaluate(() => navigator.clipboard.writeText('粘贴'));
    await page.keyboard.press('Control+v');
    await cdp.detach();
    await page.waitForTimeout(1000);
    await page.keyboard.press('Control+a');
    await page.keyboard.press('Control+c');
    let copied;
    for (let attempts = 0; attempts < 100; attempts++) {
      copied = await page.evaluate(() => navigator.clipboard.readText());
      if (copied === 'hello中文验通过🌍!粘贴') break;
      await page.waitForTimeout(50);
    }
    assert.equal(copied, 'hello中文验通过🌍!粘贴', 'remote Unicode copy reaches the local clipboard');
    await page.keyboard.press('Control+x');
    await page.keyboard.press('Control+v');
    await page.waitForTimeout(500);
    await api('/control', {acquire: false});
    const observed = await api('/instructions', {instruction: config.native ? 'observe' : 'browser eval document.title'});
    assert(JSON.stringify(observed).includes('hello中文验通过🌍!粘贴'), JSON.stringify({observed, inputTrace: await page.evaluate(() => inputTrace)}));
    // Reusing the old browser cookie after releasing control must be rejected.
    const oldPage = await context.request.get(page.url());
    assert.equal(oldPage.status(), 409);
    assert.deepEqual(errors, []);
    console.log(JSON.stringify({native: !!config.native, connected: true, fittedDesktop: true, rotation: true, pan: true, keyboardToggle: true, automaticKeyboard: true, touch: true, remoteAsciiInput: true, remoteShortcutsAndSelection: true, remoteNavigationAndDelete: true, remoteChineseIme: true, remoteEmojiAndPaste: true, remoteCopyAndCut: true, revoked: true}));
  } catch (error) {
    await page.screenshot({path: config.screenshot});
    throw error;
  } finally {
    await browser.close();
  }
})().catch(error => {console.error(error); process.exitCode = 1;});
