-- Migration to add fertilizer amount column to field yearly data

ALTER TABLE public.field_yearly_data
ADD COLUMN fertilizer_amount_kg_ha numeric NULL;
