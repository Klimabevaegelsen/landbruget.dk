# Temporary Password Protection

Simple HTML/JavaScript password protection to temporarily hide the website.

## Quick Setup

### Local Development

1. **Copy environment file**: `cp env.example .env.local`
2. **Set your password** in `.env.local`:
   ```
   NEXT_PUBLIC_SITE_PASSWORD=your_secure_password_here
   ```
3. **Start dev server**: `npm run dev`

### Production (Vercel)

1. **Go to your Vercel project settings**
2. **Add Environment Variable**:
   - Key: `NEXT_PUBLIC_SITE_PASSWORD`
   - Value: `your_secure_password_here`
3. **Redeploy** your site

## How it Works

- Password overlay covers the entire site on load
- Password comes from `NEXT_PUBLIC_SITE_PASSWORD` environment variable
- Uses localStorage to remember authentication

## To Remove Password Protection

Simply remove the password overlay div and script from `frontend/src/app/layout.tsx` (lines 23-97).

## Security Note

⚠️ **This is client-side only** - the password will be visible in the built JavaScript. This is only suitable for temporarily hiding the site, not for real security.
