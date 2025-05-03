alter table "public"."field_yearly_data" add column "company_id" uuid not null;

-- Manually added: Populate the new column after it's created
UPDATE public.field_yearly_data fyd
SET company_id = fb.company_id
FROM public.field_boundaries fb
WHERE fyd.field_boundary_id = fb.id;
-- End Manually added

alter table "public"."field_yearly_data" add constraint "fk_field_yearly_data_company" FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;

create index if not exists idx_field_yearly_data_company_id on public.field_yearly_data using btree (company_id);
