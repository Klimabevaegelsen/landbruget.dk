/**
 * Official GHS Pictogram Components
 * Using actual UN GHS pictograms from PubChem/NCBI
 * Source: https://pubchem.ncbi.nlm.nih.gov/ghs/
 */

import React from 'react';
import Image from 'next/image';

// GHS06 - Acute Toxicity (Skull and Crossbones)
export const GHS06_SkullCrossbones: React.FC<{ className?: string }> = ({
  className = 'h-4 w-4',
}) => (
  <Image
    src="/ghs-pictograms/GHS06.svg"
    alt="GHS06 Acute Toxicity - Skull and Crossbones"
    width={16}
    height={16}
    className={className}
    title="GHS06 - Acute Toxicity"
  />
);

// GHS07 - Exclamation Mark (Harmful/Irritant)
export const GHS07_ExclamationMark: React.FC<{ className?: string }> = ({
  className = 'h-4 w-4',
}) => (
  <Image
    src="/ghs-pictograms/GHS07.svg"
    alt="GHS07 Harmful/Irritant - Exclamation Mark"
    width={16}
    height={16}
    className={className}
    title="GHS07 - Harmful/Irritant"
  />
);

// GHS08 - Health Hazard (Person with starburst)
export const GHS08_HealthHazard: React.FC<{ className?: string }> = ({
  className = 'h-4 w-4',
}) => (
  <Image
    src="/ghs-pictograms/GHS08.svg"
    alt="GHS08 Health Hazard - Person with starburst"
    width={16}
    height={16}
    className={className}
    title="GHS08 - Health Hazard"
  />
);

// GHS09 - Environmental Hazard (Dead tree and fish)
export const GHS09_Environmental: React.FC<{ className?: string }> = ({
  className = 'h-4 w-4',
}) => (
  <Image
    src="/ghs-pictograms/GHS09.svg"
    alt="GHS09 Environmental Hazard - Dead tree and fish"
    width={16}
    height={16}
    className={className}
    title="GHS09 - Environmental Hazard"
  />
);

// GHS05 - Corrosive
export const GHS05_Corrosive: React.FC<{ className?: string }> = ({
  className = 'h-4 w-4',
}) => (
  <Image
    src="/ghs-pictograms/GHS05.svg"
    alt="GHS05 Corrosive - Test tube corroding hand and surface"
    width={16}
    height={16}
    className={className}
    title="GHS05 - Corrosive"
  />
);
