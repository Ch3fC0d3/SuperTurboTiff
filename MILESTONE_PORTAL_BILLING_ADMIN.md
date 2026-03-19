# Milestone: Portal, Billing, Admin, and Coupons

**Date Completed:** March 19, 2026  
**Repository:** <https://github.com/Ch3fC0d3/turbotiff>

---

## Overview

This milestone turns TurboTIFFLAS from a local-only digitization tool into a real product shell with:

- account registration and login
- pricing and checkout flows
- first-month-free trials
- billing history
- admin controls
- coupon support
- branded landing and login pages

The result is a usable portal layer for testing subscriptions, onboarding users, and preparing for live payment integration.

---

## What Was Added

### 1. Real Account System

- Replaced the old hardcoded login flow with persisted users in `portal_store.py`
- Added password hashing and session-backed login state
- Added registration with plan-aware onboarding
- Seeded the first admin account from environment variables

### 2. Pricing and Checkout

- Added real pricing, register, and checkout pages
- Added billing cycle selection
- Added structured payment records
- Added sandbox/manual billing states so the product can be exercised before Stripe

### 3. First Month Free

- Paid plans now support a `trialing` state
- Trial start and end are stored on the user record
- Billing and admin views show trial windows and days remaining

### 4. Admin Portal

- Added admin-only portal pages and guards
- Admin can review payment records
- Admin can approve pending payments
- Admin can change plan, billing cycle, subscription status, and admin access for users

### 5. Coupon System

- Added coupon storage and redemption tracking
- Supports:
  - percent discounts
  - fixed amount discounts
  - extra trial days
- Coupons can be scoped to a plan
- Coupons can be capped with redemption limits
- Coupons can expire
- Coupons can be enabled or disabled in the admin portal

### 6. Billing Visibility

- Billing history now shows:
  - final amount
  - discount amount
  - coupon code
  - status
  - provider/reference details

### 7. Brand Alignment

- Updated landing, login, and portal pages to match the new TIFLAS branding
- Added the branded shared portal shell and themed pages

---

## Files Introduced or Expanded

### Backend

- `portal_store.py`
- `web_app.py`

### Templates

- `templates/portal_base.html`
- `templates/pricing.html`
- `templates/register.html`
- `templates/checkout.html`
- `templates/billing.html`
- `templates/admin.html`
- `templates/login.html`
- `templates/index.html`
- `templates/dashboard.html`

### Assets / Support

- `static/`
- `.gitignore`

---

## Operational Notes

- Admin bootstrap uses:
  - `TURBOTIFFLAS_ADMIN_EMAIL`
  - `TURBOTIFFLAS_ADMIN_PASSWORD`
- Billing is still sandbox/manual inside the app
- Live card charging is not part of this milestone yet
- Portal databases are local runtime artifacts and are now ignored in `data/*.db`

---

## Validation Completed

- Python syntax check passed for portal modules
- Isolated smoke test passed for:
  - admin login
  - coupon creation
  - paid user signup
  - checkout with coupon
  - admin role promotion
- Local app restarted successfully with:
  - Vision enabled
  - portal routes live

---

## Next Logical Step

The next product milestone should be live billing integration, most likely Stripe, so trials, renewals, and coupon discounts can flow into real payment collection instead of sandbox/manual records.

---

**Status:** Complete  
**Deployment State:** Ready for local testing and hosted sandbox rollout
