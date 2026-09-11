const {test, expect} = require('@playwright/test');

async function login(page) {
  await page.goto('/');
  await page.getByLabel('API token', {exact:true}).fill('test-browser-token');
  await page.getByRole('button', {name:'Connect',exact:true}).click();
  await expect(page.getByLabel('Filter assets')).toBeVisible();
}

test('mixed JSON previews remain readable', async({page})=>{
  await login(page);
  await page.route('**/api/assets/sample_quality?*', route=>route.fulfill({
    json:{head:{commit_id:'component-test'},checkpoint:null,commit:null,preview:[{value:1},null,42]},
  }));
  await page.locator('.asset-list').getByRole('button',{name:'sample_quality',exact:true}).click();
  await expect(page.locator('#drawer-title')).toHaveText('sample_quality');
  await expect(page.locator('#drawer-content pre').first()).toContainText('null');
  await expect(page.locator('#drawer-content pre').first()).toContainText('42');
  await expect(page.locator('#error')).toBeHidden();
});

test('late asset responses cannot replace the current drawer', async({page})=>{
  await login(page);
  let release;
  const blocked = new Promise(resolve=>{release=resolve});
  let requested;
  const started = new Promise(resolve=>{requested=resolve});
  await page.route('**/api/assets/source_files?*', async route=>{
    requested();
    await blocked;
    await route.fulfill({json:{head:null,checkpoint:null,commit:null,preview:null}});
  });
  await page.locator('.asset-list').getByRole('button',{name:'source_files',exact:true}).click();
  await started;
  await page.locator('.asset-list').getByRole('button',{name:'sample_quality',exact:true}).click();
  await expect(page.locator('#drawer-title')).toHaveText('sample_quality');
  const arrived=page.waitForResponse(response=>response.url().includes('/api/assets/source_files?'));
  release();
  await arrived;
  await page.waitForTimeout(100);
  await expect(page.locator('#drawer-title')).toHaveText('sample_quality');
});
