'use client';

import React, { useEffect } from 'react';
import { X } from 'lucide-react';
import { FertilizerData } from '../livestock-analysis/types';
import FertilizerTrendChart from './FertilizerTrendChart';

interface FertilizerTrendPopoverProps {
  isOpen: boolean;
  onClose: () => void;
  selectedCompany: FertilizerData | null;
  selectedCompanyHistory: FertilizerData[];
}

export function FertilizerTrendPopover({ 
  isOpen, 
  onClose, 
  selectedCompany, 
  selectedCompanyHistory 
}: FertilizerTrendPopoverProps) {
  // Handle ESC key press
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape' && isOpen) {
        onClose();
      }
    };

    if (isOpen) {
      document.addEventListener('keydown', handleKeyDown);
    }

    return () => {
      document.removeEventListener('keydown', handleKeyDown);
    };
  }, [isOpen, onClose]);

  if (!isOpen || !selectedCompany) return null;

  return (
    <>
      {/* Backdrop */}
      <div 
        className="fixed inset-0 bg-black/20 backdrop-blur-sm z-40"
        onClick={onClose}
      />
      
      {/* Popover */}
      <div className="fixed inset-4 bg-white dark:bg-gray-900 rounded-lg shadow-2xl z-50 flex flex-col max-h-[90vh]">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-border">
          <div>
            <h2 className="text-xl font-semibold text-foreground">
              {selectedCompany.company_name}
            </h2>
            <div className="flex gap-4 mt-2 text-sm text-muted-foreground">
              <span><strong>CVR:</strong> {selectedCompany.cvr_number}</span>
              <span><strong>Kommune:</strong> {selectedCompany.municipality}</span>
              <span><strong>Kvælstof:</strong> {((selectedCompany.f_303_1_normproduktion_kg_n_ghi_beregnet || 0) / 1000).toFixed(1)} ton N</span>
              <span><strong>Fosfor:</strong> {((selectedCompany.f_303_3_normproduktion_kg_p_ghi_beregnet || 0) / 1000).toFixed(1)} ton P</span>
            </div>
          </div>
          <button
            onClick={onClose}
            className="p-2 rounded-full hover:bg-muted/50 transition-colors"
            aria-label="Close"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6">
          {selectedCompanyHistory.length > 1 ? (
            <FertilizerTrendChart 
              companyData={selectedCompanyHistory}
              companyName={selectedCompany.company_name}
            />
          ) : (
            <div className="flex items-center justify-center h-32">
              <div className="text-center">
                <p className="text-sm text-muted-foreground">
                  Ingen historiske data tilgængelig for denne virksomhed
                </p>
                <p className="text-xs text-muted-foreground mt-1">
                  ({selectedCompanyHistory.length} record{selectedCompanyHistory.length !== 1 ? 's' : ''} fundet)
                </p>
              </div>
            </div>
          )}
        </div>
      </div>
    </>
  );
}

export default FertilizerTrendPopover;
