import {
  translateDestinationType,
  translateOriginType,
  getAllDestinationTypeTranslations,
} from '@/lib/translations/animal-transportation';

describe('Animal Transportation Translations', () => {
  describe('translateDestinationType', () => {
    it('should translate common destination types to Danish', () => {
      expect(translateDestinationType('Slaughterhouse')).toBe('Slagteri');
      expect(translateDestinationType('Rendering Plant')).toBe(
        'Forarbejdningsanlæg'
      );
      expect(translateDestinationType('Collection Center')).toBe('Samlecenter');
      expect(translateDestinationType('Production Farm')).toBe(
        'Produktionsbesætning'
      );
      expect(translateDestinationType('Dairy Farm')).toBe('Malkekvæg');
      expect(translateDestinationType('International Export')).toBe(
        'International eksport'
      );
      expect(translateDestinationType('Unknown')).toBe('Ukendt');
    });

    it('should return original value for untranslated types', () => {
      expect(translateDestinationType('Custom Farm Type')).toBe(
        'Custom Farm Type'
      );
      expect(translateDestinationType('')).toBe('');
    });

    it('should handle all destination types from the database', () => {
      const databaseTypes = [
        'Rendering Plant',
        'Slaughterhouse',
        'Production Farm',
        'Collection Center',
        'Unknown',
        'Collection Point',
        'International Export',
        'Other Livestock',
        'Cooling Facility',
        'Beef Farm',
        'Breeding Farm',
        'Piglet Farm',
        'Hobby Farm',
        'Other Commercial',
        'Dairy Farm',
        'Organic Pig Farm',
        'Free-range Pig Farm',
        'Livestock Farm',
        'Research Facility',
        'Heifer Hotel',
        'Stud Farm',
        'Boarding/Riding School',
        'Veal Farm',
        'Racing/Training',
        'Nature Management',
        'Quarantine Facility',
        'Seasonal Grazing',
        'AI Station',
        'Livestock Show',
        'Zoo',
      ];

      // All types should have translations (no fallback to original)
      databaseTypes.forEach((type) => {
        const translated = translateDestinationType(type);
        expect(translated).not.toBe(type); // Should be translated, not original
        expect(translated).toBeTruthy(); // Should have a value
      });
    });
  });

  describe('translateOriginType', () => {
    it('should work the same as translateDestinationType', () => {
      expect(translateOriginType('Slaughterhouse')).toBe('Slagteri');
      expect(translateOriginType('Production Farm')).toBe(
        'Produktionsbesætning'
      );
    });
  });

  describe('getAllDestinationTypeTranslations', () => {
    it('should return all available translations', () => {
      const translations = getAllDestinationTypeTranslations();

      expect(translations['Slaughterhouse']).toBe('Slagteri');
      expect(translations['Production Farm']).toBe('Produktionsbesætning');
      expect(translations['Unknown']).toBe('Ukendt');

      // Should have all the main types
      expect(Object.keys(translations)).toContain('Rendering Plant');
      expect(Object.keys(translations)).toContain('International Export');
      expect(Object.keys(translations)).toContain('Dairy Farm');
    });
  });
});
