// Standalone mobile interaction regression; no desktop service required.
const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const assert = require('node:assert/strict');
const {chromium} = require('playwright');

(async () => {
  const html = fs.readFileSync(path.join(__dirname, '../../plugins/x11box-acceptance/desktop-viewer.html'), 'utf8')
    .replace('__DESKTOP_WIDTH__', '1440').replace('__DESKTOP_HEIGHT__', '900');
  let focused = false;
  const server = http.createServer((req, res) => {
    if (req.url === '/input-focus') {res.setHeader('Content-Type','application/json');res.end(JSON.stringify({focused}));return;}
    res.setHeader('Content-Type', 'text/html');
    res.end(req.url.startsWith('/index.html') ? `<!doctype html><style>html,body{margin:0;width:100%;height:100%}</style>
      <script>window.events=[];for(const type of ['mousedown','mouseup','dblclick','contextmenu'])document.addEventListener(type,e=>events.push([type,e.button]));window.packets=[];window.commits=[];window.wheels=[];window.client={connected:true,focused_wid:7,send:p=>packets.push(p),send_clipboard_token:text=>commits.push(text),packet_handlers:{'clipboard-token':function(p){if(p.length>=8)this.clipboard_buffer=p[7]}}};
      document.addEventListener('wheel',e=>wheels.push([e.clientX,e.clientY,e.deltaX,e.deltaY]));</script>` : html);
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  let browser;
  try {
    browser = await chromium.launch({executablePath: process.env.X11BOX_BROWSER_EXECUTABLE || undefined, headless: true, args: ['--no-sandbox']});
    const page = await browser.newPage({viewport: {width: 390, height: 844}, isMobile: true, hasTouch: true, locale: 'zh-CN'});
    await page.addInitScript(() => {
      window.copied = [];
      window.DesktopClipboard = {postMessage: text => copied.push(text)};
    });
    const errors = [];
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(`http://127.0.0.1:${server.address().port}/viewer`);
    const remote = await (await page.locator('#desktop').elementHandle()).contentFrame();
    await remote.waitForFunction(() => window.client?.connected);
    assert.deepEqual(await page.locator('nav button').allTextContents(), ['适应屏幕', '手机键盘']);
    const rectBeforeTap = await page.locator('#desktop').boundingBox();
    await page.touchscreen.tap(rectBeforeTap.x + 40, rectBeforeTap.y + 40);
    assert.equal(await page.locator('#native-input').isVisible(), false, 'ordinary tap must not open keyboard');
    focused = true;
    await page.touchscreen.tap(rectBeforeTap.x + 40, rectBeforeTap.y + 40);
    await page.waitForFunction(() => document.activeElement.id === 'native-input');
    await page.locator('#keyboard').click();
    await page.waitForTimeout(750);
    assert.equal(await page.locator('#native-input').isVisible(), false, 'manual dismissal stays closed while same input remains focused');
    await page.touchscreen.tap(rectBeforeTap.x + 40, rectBeforeTap.y + 40);
    await page.waitForFunction(() => document.activeElement.id === 'native-input');
    focused = false;
    await page.waitForFunction(() => !document.querySelector('#native-input').classList.contains('active'));
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
    await remote.waitForFunction(() => commits.length === 1);
    assert.deepEqual(await remote.evaluate(() => commits), ['中文']);
    assert.deepEqual(await remote.evaluate(() => packets.map(p => [p[2],p[3],p[4]])),
      [['v',true,['control']],['v',false,['control']]]);
    await page.keyboard.press('Backspace');
    await page.keyboard.press('Enter');
    await page.locator('#native-input').evaluate(el => el.dispatchEvent(new InputEvent('input', {inputType:'deleteContentBackward'})));
    await remote.waitForFunction(() => packets.length === 8);
    assert.deepEqual(await remote.evaluate(() => packets.slice(2).map(p => p[2])), ['BackSpace','BackSpace','Return','Return','BackSpace','BackSpace']);
    await remote.evaluate(() => packets.length = 0);
    await page.keyboard.press('ArrowLeft');
    await page.keyboard.press('Shift+ArrowRight');
    await page.keyboard.press('Control+a');
    await page.keyboard.press('Delete');
    assert.deepEqual(await remote.evaluate(() => packets.filter(p => p[3]).map(p => [p[2], p[4]])),
      [['Left', []], ['Right', ['shift']], ['a', ['control']], ['Delete', []]], 'hardware navigation and shortcuts reach the remote caret');
    await remote.evaluate(() => packets.length = 0);
    await page.locator('#native-input').evaluate(el => {
      el.value = '';
      el.dispatchEvent(new InputEvent('input', {inputType:'deleteContentBackward'}));
    });
    assert.deepEqual(await remote.evaluate(() => packets.map(p => p[2])), ['BackSpace', 'BackSpace'], 'Android can backspace with no locally typed text');
    assert.equal(await page.locator('#native-input').inputValue(), '\u200b');
    await remote.evaluate(() => packets.length = 0);
    await page.keyboard.insertText('teh');
    await page.locator('#native-input').evaluate(el => {
      el.value = '\u200bthe';
      el.dispatchEvent(new InputEvent('input', {inputType:'insertReplacementText'}));
    });
    assert.deepEqual(await remote.evaluate(() => packets.filter(p => p[3]).map(p => p[2])),
      ['t', 'e', 'h', 'BackSpace', 'BackSpace', 'h', 'e'], 'IME suggestions replace committed text once');
    await page.keyboard.press('End');
    await remote.evaluate(() => { packets.length = 0; commits.length = 0; });
    const ime = await page.context().newCDPSession(page);
    await ime.send('Input.imeSetComposition', {text:'中文', selectionStart:2, selectionEnd:2});
    assert.equal(await remote.evaluate(() => packets.length), 0, 'actual browser composition stays local until committed');
    await ime.send('Input.insertText', {text:'中文'});
    await remote.waitForFunction(() => commits.length === 1);
    assert.deepEqual(await remote.evaluate(() => commits), ['中文'], 'actual IME commit is forwarded once');
    await ime.detach();
    await page.keyboard.press('Control+c');
    await remote.waitForFunction(() => packets.some(p => p[2] === 'c' && p[4].includes('control')));
    await remote.evaluate(() => client.packet_handlers['clipboard-token'].call(client, ['clipboard-token', 'CLIPBOARD', []]));
    assert.deepEqual(await page.evaluate(() => copied), [], 'clipboard advertisements must not copy stale buffered text');
    await remote.evaluate(() => client.packet_handlers['clipboard-token'].call(client, ['clipboard-token', 'CLIPBOARD', ['UTF8_STRING'], 'UTF8_STRING', 'UTF8_STRING', 8, 'bytes', '复制内容']));
    assert.deepEqual(await page.evaluate(() => copied), ['复制内容'], 'remote copy reaches the native clipboard bridge');
    await remote.evaluate(() => {
      client.packet_handlers['clipboard-token'] = function(packet) { if (packet.length >= 8) this.clipboard_buffer = packet[7]; };
    });
    await page.keyboard.press('Control+c');
    await remote.evaluate(() => client.packet_handlers['clipboard-token'].call(client, ['clipboard-token', 'CLIPBOARD', ['UTF8_STRING'], 'UTF8_STRING', 'UTF8_STRING', 8, 'bytes', '重连后复制']));
    assert.deepEqual(await page.evaluate(() => copied), ['复制内容', '重连后复制'], 'clipboard bridge survives Xpra handler initialization and reconnect');
    await page.locator('#keyboard').click();
    assert.equal(await page.locator('#native-input').isVisible(), false);
    const cdp = await page.context().newCDPSession(page);
    const touchBox = await page.locator('#desktop').boundingBox();
    const finger = (y) => [{x: touchBox.x + 100, y: touchBox.y + y}];
    await cdp.send('Input.dispatchTouchEvent', {type:'touchStart',touchPoints:finger(160)});
    await cdp.send('Input.dispatchTouchEvent', {type:'touchMove',touchPoints:finger(80)});
    await cdp.send('Input.dispatchTouchEvent', {type:'touchEnd',touchPoints:[]});
    assert.equal(await remote.evaluate(() => wheels.length), 0, 'single finger never scrolls remote content');
    assert.equal(await page.locator('#native-input').isVisible(), false);
    const initialViewport = await page.locator('#viewport').boundingBox();
    const initialPair = spread => [{x:195-spread,y:initialViewport.y+120},{x:195+spread,y:initialViewport.y+120}];
    const initialScale = await page.locator('#zoom').textContent();
    await cdp.send('Input.dispatchTouchEvent', {type:'touchStart',touchPoints:initialPair(35)});
    await cdp.send('Input.dispatchTouchEvent', {type:'touchMove',touchPoints:initialPair(110)});
    await cdp.send('Input.dispatchTouchEvent', {type:'touchEnd',touchPoints:[]});
    assert(parseFloat(await page.locator('#zoom').textContent()) > parseFloat(initialScale), 'pinch enlarges the fitted desktop');
    const enlarged = await page.locator('#viewport').boundingBox();
    const dragFinger = x => [{x, y: enlarged.y + 100}];
    await cdp.send('Input.dispatchTouchEvent', {type:'touchStart',touchPoints:dragFinger(330)});
    await cdp.send('Input.dispatchTouchEvent', {type:'touchMove',touchPoints:dragFinger(30)});
    await cdp.send('Input.dispatchTouchEvent', {type:'touchEnd',touchPoints:[]});
    assert(await page.locator('#viewport').evaluate(el => el.scrollLeft > 0), 'single finger pans enlarged desktop');
    const scrollBefore = await remote.evaluate(() => wheels.length);
    const two = y => [{x:100,y},{x:180,y}];
    await cdp.send('Input.dispatchTouchEvent', {type:'touchStart',touchPoints:two(enlarged.y+150)});
    await cdp.send('Input.dispatchTouchEvent', {type:'touchMove',touchPoints:two(enlarged.y+80)});
    await cdp.send('Input.dispatchTouchEvent', {type:'touchEnd',touchPoints:[]});
    assert(await remote.evaluate(n => wheels.length > n, scrollBefore), 'two fingers scroll remote content while zoomed');
    // Always start gestures in the visible viewport, including after inertia.
    const visible = await page.locator('#viewport').boundingBox();
    const cy = visible.y + 120;
    const pairAt = spread => [{x:195-spread,y:cy},{x:195+spread,y:cy}];
    const scaleBefore = await page.locator('#zoom').textContent();
    await cdp.send('Input.dispatchTouchEvent', {type:'touchStart',touchPoints:pairAt(35)});
    await cdp.send('Input.dispatchTouchEvent', {type:'touchMove',touchPoints:pairAt(65)});
    await cdp.send('Input.dispatchTouchEvent', {type:'touchEnd',touchPoints:[]});
    assert.notEqual(await page.locator('#zoom').textContent(), scaleBefore, 'pinch changes zoom');
    await cdp.send('Input.dispatchTouchEvent', {type:'touchStart',touchPoints:pairAt(35)});
    await cdp.send('Input.dispatchTouchEvent', {type:'touchEnd',touchPoints:[]});
    assert(await remote.evaluate(() => events.some(e => e[0]==='mousedown' && e[1]===2)), 'two finger tap sends right button');
    await page.touchscreen.tap(195,cy);
    await page.touchscreen.tap(195,cy);
    assert(await remote.evaluate(() => events.some(e => e[0]==='dblclick')), 'double tap');
    const ups = await remote.evaluate(() => events.filter(e=>e[0]==='mouseup').length);
    await cdp.send('Input.dispatchTouchEvent', {type:'touchStart',touchPoints:[{x:195,y:cy}]});
    await page.waitForTimeout(500);
    await cdp.send('Input.dispatchTouchEvent', {type:'touchMove',touchPoints:[{x:220,y:cy+20}]});
    await cdp.send('Input.dispatchTouchEvent', {type:'touchCancel',touchPoints:[]});
    assert.equal(await remote.evaluate(() => events.filter(e=>e[0]==='mouseup').length), ups+1, 'cancel releases remote drag');
    await page.locator('#keyboard').click();
    await page.getByRole('button',{name:'Ctrl',exact:true}).click();
    await page.getByRole('button',{name:'Tab',exact:true}).click();
    assert(await remote.evaluate(() => packets.some(p=>p[2]==='Tab'&&p[4].includes('control'))), 'modifier included');
    await page.locator('#keyboard').click();
    assert(await remote.evaluate(() => packets.some(p=>p[2]==='Control_L'&&p[3]===false)), 'closing keyboard releases modifier');
    await remote.evaluate(() => client.connected=false);
    await page.waitForFunction(() => document.querySelector('#connection').textContent.includes('重连'));
    await remote.evaluate(() => client.connected=true);
    await page.waitForFunction(() => document.querySelector('#connection').textContent==='已连接');
    await page.locator('#fit').click();
    await page.setViewportSize({width:844,height:390});
    await page.waitForTimeout(100);
    const rect = await page.locator('#desktop').boundingBox();
    assert(rect.x + rect.width <= 845 && rect.y + rect.height <= 391);
    assert.deepEqual(errors, []);
    console.log('PASS: simplified toolbar, focus/dismissal, actual IME commit and replacement, empty-buffer deletion, hardware shortcuts/navigation, modifier release, single-finger pan, two-finger scroll, pinch zoom, right/double click, drag cancel, connection status, landscape fit');
  } finally {
    await browser?.close();
    await new Promise(resolve => server.close(resolve));
  }
})().catch(e => {console.error(e);process.exitCode=1;});
