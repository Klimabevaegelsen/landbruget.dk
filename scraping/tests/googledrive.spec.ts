import { test } from '@playwright/test';
import fs from 'fs';

test('extract folder ids', async ({ page }) => {
  const googleDriveUrl = 'https://drive.google.com/drive/folders/18-Tvu8pX5ZCLAfZyRujGT8JRa5uRLKUz';
  
  const navigateAndGetLinks = async (url) => {
    await page.goto(url);
    
    const folderElements = await page.$$('[data-target="folder"]');
    const docElements = await page.$$('[data-target="doc"]');
    const folders = await Promise.all(
      folderElements.map(async element => await element.getAttribute('data-id'))
    )
    const docs = await  Promise.all(
      docElements.map(async element => await element.getAttribute('data-id'))
    );
    
    return {
      folders: folders.map(id => `https://drive.google.com/drive/folders/${id}\n`),
      docs: docs.map(id => `https://drive.google.com/drive/folders/${id}\n`),
      // pageContent: await page.evaluate('document.body.innerHTML')
    }
  }
  const links = await navigateAndGetLinks(googleDriveUrl)
  fs.writeFileSync('./scraping/google_drive/folder_ids.json', JSON.stringify(links, null, 2));
});