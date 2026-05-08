# شُعرة (Sharrah)

واجهة عربية RTL لعرض ريلز فيسبوك كـ “بطاقات” (مع صورة مصغّرة ورابط خارجي).

المشروع الآن يعتمد فقط على **Facebook Graph API** لتعبئة قاعدة البيانات محليًا (SQLite)، ولا يحتوي على أي **سكرابر** أو **داونلودر**.

## التشغيل

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn api.app:app --host 0.0.0.0 --port 8000 --reload
```

من نفس الجهاز افتح: `http://127.0.0.1:8000/sharah`

من الهاتف على نفس الشبكة افتح: `http://<VM_IP>:8000/sharah`

يمكنك أيضًا تشغيل نفس الأمر عبر:

```bash
./quickstart.sh
```

## مصدر الريلز (Excel أو قاعدة البيانات)

افتراضيًا `SHARAH_REELS_SOURCE=auto` وسيتم استخدام ملف `shadi_shirri_reels.xlsx` إن كان موجودًا، وإلا سيتم القراءة من SQLite.

متغيرات البيئة:

- `SHARAH_REELS_SOURCE`: `auto` أو `excel` أو `db`
- `SHARAH_REELS_XLSX`: مسار ملف الإكسل (الافتراضي: `shadi_shirri_reels.xlsx`)

## إعدادات Graph API

انسخ `.env.example` إلى `.env` ثم اضبط:

- `FB_PAGE_ID`
- `FB_PAGE_ACCESS_TOKEN`
- `FB_GRAPH_API_VERSION` (اختياري)

## API

- `GET /api/sharah/reels` قائمة ريلز للعرض مع العنوان إن كان موجودًا في الإكسل/قاعدة البيانات
- `GET /api/sharah/reels/search?q=...` بحث هجين: كلمات مفتاحية + تشابه عناوين محفوظة كـ embeddings في SQLite
- `POST /api/sharah/reels/index-embeddings` إنشاء/تحديث embeddings لعناوين الريلز الموجودة
- `POST /api/sharah/reels/sync-graph` مزامنة/تحديث قاعدة البيانات من Graph API
- `GET /api/sharah/reels/from-db` عرض بيانات قاعدة البيانات (يتضمن العنوان)

## تحديث عناوين الريلز

بعد ملء عمود `reel_title` في ملف الإكسل، يمكن فهرسة العناوين للبحث:

```bash
python3 fb_reel_title_browser.py --update-xlsx --limit 20
curl -X POST http://127.0.0.1:8000/api/sharah/reels/index-embeddings
```
