-- ==========================================================
-- קובץ אתחול נקי: רק הגדרות תשתית, ללא תושבים
-- ==========================================================

-- 1. יצירת רחוב ברירת מחדל 999 (חובה למניעת קריסה בטעינה ראשונית)
INSERT INTO public.street (streetcode, streetname) VALUES (999, 'רחוב כללי')
ON CONFLICT (streetcode) DO NOTHING;

-- 2. הגדרת מחיר משלוח ברירת מחדל
INSERT INTO public.delivery_settings (setting_name, setting_value) VALUES ('delivery_price', 15.00)
ON CONFLICT (setting_name) DO UPDATE SET setting_value = EXCLUDED.setting_value;

-- 3. איפוס רצפים (Sequences) להתחלה נקייה
-- זה מבטיח שהתושב הראשון שייקלט יקבל את המספר 1 (או את הקוד שלו מהקובץ)
SELECT setval('public.person_personid_seq', 1, false);
SELECT setval('public.street_streetcode_seq', 1000, false);