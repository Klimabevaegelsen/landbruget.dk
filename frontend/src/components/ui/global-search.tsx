'use client';

import { useState, useEffect, useCallback } from 'react';
import {
  CommandDialog,
  CommandInput,
  CommandList,
  CommandEmpty,
  CommandGroup,
  CommandItem,
} from '@/components/ui/command';
import {
  Search,
  Building2,
  MapPin,
  Wheat,
  TrendingUp,
  FileText,
  Settings,
} from 'lucide-react';
import { useRouter } from 'next/navigation';

interface SearchResult {
  id: string;
  title: string;
  description?: string;
  type:
    | 'company'
    | 'municipality'
    | 'field'
    | 'analysis'
    | 'document'
    | 'navigation';
  url: string;
  icon?: React.ReactNode;
}

// Mock data - in real implementation, this would come from API
const mockSearchResults: SearchResult[] = [
  {
    id: '1',
    title: 'Dansk Landbrug A/S',
    description: 'CVR: 12345678 • Hovedgård Kommune',
    type: 'company',
    url: '/virksomheder/12345678',
    icon: <Building2 className="h-4 w-4" />,
  },
  {
    id: '2',
    title: 'Økologisk Gård ApS',
    description: 'CVR: 87654321 • Roskilde Kommune',
    type: 'company',
    url: '/virksomheder/87654321',
    icon: <Building2 className="h-4 w-4" />,
  },
  {
    id: '3',
    title: 'Københavns Kommune',
    description: '158 virksomheder • 2,847 marker',
    type: 'municipality',
    url: '/kommuner/101',
    icon: <MapPin className="h-4 w-4" />,
  },
  {
    id: '4',
    title: 'Aarhus Kommune',
    description: '342 virksomheder • 5,234 marker',
    type: 'municipality',
    url: '/kommuner/751',
    icon: <MapPin className="h-4 w-4" />,
  },
  {
    id: '5',
    title: 'Mark 12-4-567',
    description: 'Dansk Landbrug A/S • 15.2 ha • Vinterhvede',
    type: 'field',
    url: '/marker/12-4-567',
    icon: <Wheat className="h-4 w-4" />,
  },
  {
    id: '6',
    title: 'Pesticid Analyse 2024',
    description: 'Landsdækkende analyse af pesticidanvendelse',
    type: 'analysis',
    url: '/analyser/pesticide-2024',
    icon: <TrendingUp className="h-4 w-4" />,
  },
  {
    id: '7',
    title: 'CHR Veterinær Data',
    description: 'Kvægregistret og veterinærbehandlinger',
    type: 'document',
    url: '/dokumentation/chr',
    icon: <FileText className="h-4 w-4" />,
  },
  {
    id: '8',
    title: 'Dashboard Indstillinger',
    description: 'Konfigurer dit personlige dashboard',
    type: 'navigation',
    url: '/indstillinger',
    icon: <Settings className="h-4 w-4" />,
  },
];

export function GlobalSearch() {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const [filteredResults, setFilteredResults] = useState<SearchResult[]>([]);
  const router = useRouter();

  // Global keyboard shortcut (Cmd+K / Ctrl+K)
  useEffect(() => {
    const down = (e: KeyboardEvent) => {
      if (e.key === 'k' && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        setOpen((open) => !open);
      }
    };

    document.addEventListener('keydown', down);
    return () => document.removeEventListener('keydown', down);
  }, []);

  // Filter results based on query
  useEffect(() => {
    if (!query) {
      setFilteredResults(mockSearchResults);
      return;
    }

    const filtered = mockSearchResults.filter(
      (result) =>
        result.title.toLowerCase().includes(query.toLowerCase()) ||
        result.description?.toLowerCase().includes(query.toLowerCase())
    );
    setFilteredResults(filtered);
  }, [query]);

  const handleSelect = useCallback(
    (result: SearchResult) => {
      setOpen(false);
      setQuery('');
      router.push(result.url);
    },
    [router]
  );

  const groupedResults = filteredResults.reduce(
    (acc, result) => {
      if (!acc[result.type]) {
        acc[result.type] = [];
      }
      acc[result.type].push(result);
      return acc;
    },
    {} as Record<string, SearchResult[]>
  );

  const getGroupTitle = (type: string): string => {
    const titles = {
      company: 'Virksomheder',
      municipality: 'Kommuner',
      field: 'Marker',
      analysis: 'Analyser',
      document: 'Dokumentation',
      navigation: 'Navigation',
    };
    return titles[type as keyof typeof titles] || type;
  };

  return (
    <>
      {/* Search Trigger Button */}
      <button
        onClick={() => setOpen(true)}
        className="text-muted-foreground bg-background border-border hover:bg-accent hover:text-accent-foreground inline-flex items-center gap-2 rounded-md border px-3 py-2 text-sm transition-colors"
      >
        <Search className="h-4 w-4" />
        <span>Søg...</span>
        <kbd className="bg-muted text-muted-foreground pointer-events-none inline-flex h-5 items-center gap-1 rounded border px-1.5 font-mono text-[10px] font-medium opacity-100 select-none">
          <span className="text-xs">⌘</span>K
        </kbd>
      </button>

      {/* Command Dialog */}
      <CommandDialog open={open} onOpenChange={setOpen}>
        <CommandInput
          placeholder="Søg virksomheder, marker, kommuner..."
          value={query}
          onValueChange={setQuery}
        />
        <CommandList>
          <CommandEmpty>Ingen resultater fundet.</CommandEmpty>

          {Object.entries(groupedResults).map(([type, results]) => (
            <CommandGroup key={type} heading={getGroupTitle(type)}>
              {results.map((result) => (
                <CommandItem
                  key={result.id}
                  value={result.title}
                  onSelect={() => handleSelect(result)}
                  className="flex items-center gap-3 px-3 py-2"
                >
                  {result.icon}
                  <div className="flex flex-col">
                    <span className="font-medium">{result.title}</span>
                    {result.description && (
                      <span className="text-muted-foreground text-xs">
                        {result.description}
                      </span>
                    )}
                  </div>
                </CommandItem>
              ))}
            </CommandGroup>
          ))}
        </CommandList>
      </CommandDialog>
    </>
  );
}
