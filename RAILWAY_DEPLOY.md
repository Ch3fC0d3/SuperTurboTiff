# 🚂 Deploy to Railway.app (Supports Large Files!)

Railway.app is better for your TurboTIFFLAS app because:
- ✅ **100MB+ file uploads** (vs Vercel's 4.5MB limit)
- ✅ **Longer processing time** (no 10-second timeout)
- ✅ **Real server** (not serverless functions)
- ✅ **$5 free credit/month** (enough for moderate usage)

## 🚀 Deploy Steps

### Option 1: Deploy from GitHub (Recommended)

1. **Push code to GitHub** (if not already):
   ```bash
   cd D:\Users\gabep\Desktop\turbotifflas
   git init
   git add .
   git commit -m "TIFF to LAS web app"
   git branch -M main
   git remote add origin https://github.com/yourusername/turbotifflas.git
   git push -u origin main
   ```

2. **Deploy on Railway**:
   - Go to https://railway.app
   - Click "Start a New Project"
   - Choose "Deploy from GitHub repo"
   - Select your `turbotifflas` repository
   - Railway auto-detects Python and deploys!

3. **Get your URL**:
   - Railway gives you a URL like: `https://turbotifflas-production.up.railway.app`
   - Share this with users!

### Option 2: Deploy from CLI

1. **Install Railway CLI**:
   ```bash
   npm install -g @railway/cli
   ```

2. **Login and deploy**:
   ```bash
   cd D:\Users\gabep\Desktop\turbotifflas
   railway login
   railway init
   railway up
   ```

3. **Open your app**:
   ```bash
   railway open
   ```

## 📊 What's Different from Vercel

| Feature | Vercel | Railway |
|---------|--------|---------|
| Max file size | 4.5MB | 100MB+ |
| Timeout | 10 seconds | 120+ seconds |
| Type | Serverless | Real server |
| Free tier | Unlimited | $5/month credit |
| Best for | Small files | Large TIFFs ✅ |

## 🔧 Configuration

Railway uses these files (already created):
- `requirements.txt` - Python dependencies
- `web_app.py` - Flask app (runs on port from $PORT env var)
- `railway.json` - Deployment config (optional)
- `deploy_models/testtiflas_black_seg_v2_pairs_wvgs.pt` - tracked fallback black model used on GitHub deploys

## 💰 Cost Estimate

**Free tier**: $5 credit/month
- Typical usage: ~$0.50-2/month for light use
- Your $5 credit covers 2-10 months depending on traffic

## 🎯 After Deployment

1. Test with a large TIFF file (up to 100MB)
2. Share the Railway URL with users
3. Monitor usage in Railway dashboard

## 🔒 Environment Variables (Optional)

If you want to add Google Vision API later:
1. Railway Dashboard → Your Project → Variables
2. Add: `GOOGLE_APPLICATION_CREDENTIALS` = (paste JSON key)

### Recommended for persistent training captures

If you want `Save bad black segment`, `Save corrected black segment`, and the new automatic black snapshots to survive redeploys:

1. Add a Railway Volume
2. Mount it at a path such as `/data`
3. Add this variable:

```env
TURBOTIFFLAS_TRAINING_CAPTURES_DIR=/data/training_captures
```

Optional if you later want to load a model from the volume:

```env
CURVE_TRACE_MODEL_PATH=/data/models/your_model.pt
RAILWAY_VOLUME_MOUNT_PATH=/data
```

## 🚦 Ready to Deploy?

Choose Option 1 (GitHub) for easiest deployment and automatic updates when you push code changes!
