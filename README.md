# AI Financial News

واجهة Streamlit لعرض الأخبار من الكاش فقط، مع خط معالجة منفصل لتحديث الكاش بشكل مجدول.

## الفكرة العامة

- `app.py`:
  - يعرض الأخبار من `cache/latest.json` فقط.
  - لا يستدعي جلب الأخبار من الشبكة عند تحديث الصفحة.
  - زر `Refresh Feed` يعيد قراءة ملف الكاش من القرص فقط.
- `pipeline_scheduler.py`:
  - مسؤول عن جلب الأخبار وتحديث ملفات الكاش.
  - يحفظ:
    - `cache/latest.json`
    - نسخة احتياطية بصيغة `cache/YYYY-MM-DD_HH-MM.json`

## المتطلبات

- Python 3.10+ (يفضل 3.11)
- تثبيت الحزم:

```bash
pip install -r requirements.txt
```

## تشغيل الواجهة (قراءة من الكاش فقط)

```bash
streamlit run app.py
```

إذا لم يوجد `cache/latest.json` ستظهر رسالة تطلب تشغيل خط التحديث.

## تشغيل خط التحديث يدويًا

تشغيل مرة واحدة (مناسب للسيرفر/الجدولة):

```bash
python pipeline_scheduler.py --once
```

تشغيل مستمر كل 30 دقيقة (اختياري):

```bash
python pipeline_scheduler.py --loop --interval 1800
```

## تهيئة التشغيل المجدول من السيرفر

الأنسب في بيئة السيرفر: استدعاء `--once` من المجدول (cron أو Task Scheduler) بدل حلقة لا نهائية.

### Linux (cron)

افتح crontab:

```bash
crontab -e
```

شغل كل 30 دقيقة:

```cron
*/30 * * * * cd /path/to/project && /usr/bin/python3 pipeline_scheduler.py --once >> /path/to/project/cache/scheduler.log 2>&1
```

### Windows (Task Scheduler)

1. Create Task
2. Trigger: Daily + Repeat task every `30 minutes`
3. Action:
   - Program/script: `python`
   - Add arguments: `pipeline_scheduler.py --once`
   - Start in: `C:\path\to\project`

## منع التداخل (Overlapping Runs)

`pipeline_scheduler.py` يستخدم قفل ملف:

- `cache/.scheduler.lock`

هذا يمنع تشغيلين بنفس الوقت إذا السيرفر أرسل أكثر من trigger.

## الملفات المهمة

- `app.py`: واجهة العرض والمراجعة والتصدير.
- `pipeline_scheduler.py`: جلب الأخبار وتحديث الكاش.
- `cache/latest.json`: أحدث بيانات جاهزة للواجهة.
- `cache/YYYY-MM-DD_HH-MM.json`: نسخ احتياطية زمنية.
