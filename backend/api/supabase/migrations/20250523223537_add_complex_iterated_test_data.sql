-- Migration to add complex test data for iteratedSections testing
-- This adds a second production site and multiple species for both sites
-- to create proper tree structure: 2 level 1 items (sites), each with 2+ sub items (species)

DO $$
DECLARE
    test_company_id uuid := '180d0a9f-7e56-499c-8cd7-7832aeae0e2d'; -- Existing "Komplet Testfarm ApS" company ID
BEGIN
    -- Add a second production site for the test company
    INSERT INTO public.production_sites (chr, company_id, site_name, address, postal_code, city, municipality, location_geom, main_species_id, capacity) 
    VALUES
    ('CHR00402', test_company_id, 'Kolding Kvægfarm', 'Skovvej 25', '6000', 'Kolding', 'Kolding', ST_GeomFromText('POINT(9.475 55.495)', 4326), 102, 300)
    ON CONFLICT (chr) DO NOTHING;

    -- Check if we already have multiple species data
    IF NOT EXISTS (SELECT 1 FROM animal_production_log WHERE chr = 'CHR00401' AND species_name != 'Pig') THEN
        -- Add additional species data for CHR00401 (existing site) - Add Kvæg and Fjerkræ
        INSERT INTO public.animal_production_log (chr, year, species_id, species_name, age_group, production_volume_equiv) VALUES
        -- Kvæg for CHR00401
        ('CHR00401', 2023, 102, 'Kvæg', 'Kalve', 80),
        ('CHR00401', 2023, 102, 'Kvæg', 'Kvier', 50),
        ('CHR00401', 2023, 102, 'Kvæg', 'Malkekøer', 120),
        ('CHR00401', 2022, 102, 'Kvæg', 'Kalve', 75),
        ('CHR00401', 2022, 102, 'Kvæg', 'Kvier', 45),
        ('CHR00401', 2022, 102, 'Kvæg', 'Malkekøer', 110),
        ('CHR00401', 2021, 102, 'Kvæg', 'Kalve', 70),
        ('CHR00401', 2021, 102, 'Kvæg', 'Malkekøer', 100),
        
        -- Fjerkræ for CHR00401  
        ('CHR00401', 2023, 103, 'Fjerkræ', 'Slagtekyllinger', 2500),
        ('CHR00401', 2023, 103, 'Fjerkræ', 'Høner', 800),
        ('CHR00401', 2022, 103, 'Fjerkræ', 'Slagtekyllinger', 2200),
        ('CHR00401', 2022, 103, 'Fjerkræ', 'Høner', 750),
        ('CHR00401', 2021, 103, 'Fjerkræ', 'Slagtekyllinger', 2000),
        ('CHR00401', 2021, 103, 'Fjerkræ', 'Høner', 700);
    END IF;

    -- Check if we already have data for CHR00402
    IF NOT EXISTS (SELECT 1 FROM animal_production_log WHERE chr = 'CHR00402') THEN
        -- Add animal production data for CHR00402 (new site) - Kvæg and Svin
        INSERT INTO public.animal_production_log (chr, year, species_id, species_name, age_group, production_volume_equiv) VALUES
        -- Kvæg for CHR00402 (primary species)
        ('CHR00402', 2023, 102, 'Kvæg', 'Kalve', 150),
        ('CHR00402', 2023, 102, 'Kvæg', 'Kvier', 90),
        ('CHR00402', 2023, 102, 'Kvæg', 'Malkekøer', 200),
        ('CHR00402', 2023, 102, 'Kvæg', 'Tyre', 5),
        ('CHR00402', 2022, 102, 'Kvæg', 'Kalve', 140),
        ('CHR00402', 2022, 102, 'Kvæg', 'Kvier', 85),
        ('CHR00402', 2022, 102, 'Kvæg', 'Malkekøer', 190),
        ('CHR00402', 2022, 102, 'Kvæg', 'Tyre', 4),
        ('CHR00402', 2021, 102, 'Kvæg', 'Kalve', 130),
        ('CHR00402', 2021, 102, 'Kvæg', 'Malkekøer', 180),
        
        -- Svin for CHR00402 (secondary species)
        ('CHR00402', 2023, 101, 'Svin', 'Smågrise', 300),
        ('CHR00402', 2023, 101, 'Svin', 'Slagtesvin', 150),
        ('CHR00402', 2023, 101, 'Svin', 'Søer', 25),
        ('CHR00402', 2022, 101, 'Svin', 'Smågrise', 280),
        ('CHR00402', 2022, 101, 'Svin', 'Slagtesvin', 140),
        ('CHR00402', 2022, 101, 'Svin', 'Søer', 22),
        ('CHR00402', 2021, 101, 'Svin', 'Smågrise', 250),
        ('CHR00402', 2021, 101, 'Svin', 'Slagtesvin', 130);
    END IF;

    -- Add site yearly summary data for both sites
    INSERT INTO public.site_yearly_summary (chr, year, owner_cvr, capacity, current_disease_status, production_equiv, antibiotics_ddd, transport_count) VALUES
    ('CHR00401', 2023, '99887766', 350, 'Clear', 15530, 75, 95),
    ('CHR00401', 2022, '99887766', 350, 'Clear', 15105, 70, 88),
    ('CHR00401', 2021, '99887766', 350, 'Clear', 12870, 65, 80),
    ('CHR00402', 2023, '99887766', 300, 'Clear', 1120, 45, 65),
    ('CHR00402', 2022, '99887766', 300, 'Clear', 1057, 42, 60),
    ('CHR00402', 2021, '99887766', 300, 'Clear', 990, 38, 55)
    ON CONFLICT (chr, year) DO NOTHING;

    -- Add some veterinary events for both sites to make the data more realistic
    INSERT INTO public.vet_events (chr, event_date, event_type, description, species_id) VALUES
    ('CHR00401', '2023-03-15 10:00:00+00', 'Vaccination', 'Årlig svinevaccination', 101),
    ('CHR00401', '2023-06-20 14:30:00+00', 'Health Check', 'Kvæg sundhedsinspektion', 102),
    ('CHR00401', '2023-09-10 09:15:00+00', 'Treatment', 'Fjerkræ antibiotikabehandling', 103),
    ('CHR00401', '2023-11-05 16:00:00+00', 'Vaccination', 'Sæsonbetonet kvægvaccination', 102),
    ('CHR00402', '2023-04-10 09:00:00+00', 'Vaccination', 'Kvæg vaccinationsprogram', 102),
    ('CHR00402', '2023-05-25 11:15:00+00', 'Health Check', 'Svin sundhedstjek', 101),
    ('CHR00402', '2023-08-12 16:00:00+00', 'Treatment', 'Kvæg mastitisbehandling', 102),
    ('CHR00402', '2023-10-18 14:30:00+00', 'Vaccination', 'Svinevaccination', 101);

    RAISE NOTICE 'Complex iteratedSections test data added successfully.';
    RAISE NOTICE 'CHR00401 (Vejle Svinefarm) now has: Pig, Kvæg, Fjerkræ';
    RAISE NOTICE 'CHR00402 (Kolding Kvægfarm) now has: Kvæg, Svin';
    RAISE NOTICE 'This creates 2 level 1 items (sites) with 2+ species each for proper testing.';
END $$; 