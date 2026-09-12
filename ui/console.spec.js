const {test, expect} = require('@playwright/test');
async function login(page) {
  await page.goto('/');
  await page.getByLabel('API token', {exact:true}).fill('test-browser-token');
  await page.getByRole('button', {name:'Connect',exact:true}).click();
  await expect(page.getByRole('heading',{name:'Assets',exact:true})).toBeVisible();
  await expect(page.getByLabel('Filter assets')).toBeVisible();
}
test('catalog, filtering, storage, and responsive layout', async({page}, testInfo)=>{
  await login(page);
  await page.getByLabel('Filter assets').fill('sample_quality');
  await expect(page.locator('.asset-list tbody tr')).toHaveCount(1);
  await page.getByLabel('Filter assets').fill('');
  await expect(page.locator('.asset-list tbody tr')).toHaveCount(7);
  await page.screenshot({path:testInfo.outputPath('catalog.png'),fullPage:true});
  await page.getByRole('button',{name:'Storage',exact:true}).click();
  await expect(page.getByText('Local filesystem',{exact:true})).toBeVisible();
  await expect(page.getByText('SlateDB 0.16',{exact:true})).toBeVisible();
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth)).toBeTruthy();
});
test('materialize a real multi-output DAG and inspect data', async({page},testInfo)=>{
  await login(page);
  await page.getByRole('button',{name:'Materialize',exact:true}).click();
  await page.getByRole('checkbox',{name:'sample_quality',exact:true}).check();
  await page.getByRole('button',{name:'Start materialization',exact:true}).click();
  await expect(page.locator('#drawer-content > .tag')).toHaveText('succeeded',{timeout:70000});
  await page.screenshot({path:testInfo.outputPath('run.png'),fullPage:true});
  await page.locator('#drawer').getByRole('button',{name:'Close',exact:true}).click();
  await page.getByRole('button',{name:/^Assets/}).click();
  await page.locator('.asset-list').getByRole('button',{name:'sample_quality',exact:true}).click();
  await expect(page.getByRole('heading',{name:'Data preview · first 100 rows'})).toBeVisible();
  await expect(page.getByRole('cell',{name:'Basalt A',exact:true})).toBeVisible();
  await page.screenshot({path:testInfo.outputPath('asset.png'),fullPage:true});
});
test('bounded backfill form submits real daily work', async({page})=>{
  await login(page);
  await page.getByRole('button',{name:'Materialize',exact:true}).click();
  await page.getByRole('checkbox',{name:'daily_report',exact:true}).check();
  await page.getByLabel('From',{exact:true}).fill('2026-01-01');
  await page.getByLabel('Through',{exact:true}).fill('2026-01-02');
  await page.getByRole('button',{name:'Start materialization',exact:true}).click();
  await expect(page.locator('#drawer-content > .tag')).toHaveText('succeeded',{timeout:70000});
  await expect(page.locator('#drawer-content tbody tr')).toHaveCount(4);
});
test('automation controls and validation', async({page})=>{
  await login(page);
  await page.getByRole('button',{name:'Automations',exact:true}).click();
  const row=page.locator('tbody tr',{hasText:'refresh_laboratory'});
  await row.getByRole('button',{name:'Paused — enable',exact:true}).click();
  await expect(row.getByRole('button',{name:'Enabled — pause',exact:true})).toBeVisible();
  await row.getByRole('button',{name:'Enabled — pause',exact:true}).click();
  await expect(row.getByRole('button',{name:'Paused — enable',exact:true})).toBeVisible();
  await row.getByRole('button',{name:'Run now',exact:true}).click();
  await expect(page.locator('#drawer-content')).toBeVisible();
  await page.locator('#drawer').getByRole('button',{name:'Close',exact:true}).click();
  await page.getByRole('button',{name:'Materialize',exact:true}).click();
  await page.getByRole('button',{name:'Start materialization',exact:true}).click();
  await expect(page.locator('#request-error')).toHaveText('Select at least one asset');
});
