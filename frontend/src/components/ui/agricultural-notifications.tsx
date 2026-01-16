'use client';

import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { AlertTriangle, CheckCircle, Info, TrendingUp } from 'lucide-react';

// Notification types for data platform context
export type NotificationType =
  | 'analysis_complete'
  | 'export_ready'
  | 'data_sync'
  | 'system_error'
  | 'data_updated';

interface NotificationData {
  title: string;
  description?: string;
  action?: {
    label: string;
    onClick: () => void;
  };
}

// Data platform notification presets
export const dataNotifications = {
  analysisComplete: (
    analysisType: string,
    resultCount: number
  ): NotificationData => ({
    title: `${analysisType} analyse færdig`,
    description: `${resultCount} resultater er klar til gennemsyn`,
    action: {
      label: 'Se resultater',
      onClick: () => console.log('Navigate to analysis results'),
    },
  }),

  exportReady: (
    exportType: string,
    recordCount: number,
    fileSize: string
  ): NotificationData => ({
    title: `${exportType} eksport klar`,
    description: `${recordCount} poster (${fileSize}) er klar til download`,
    action: {
      label: 'Download',
      onClick: () => console.log('Download export file'),
    },
  }),

  dataSync: (source: string, recordCount: number): NotificationData => ({
    title: `Data synkronisering færdig`,
    description: `${recordCount} poster fra ${source} er opdateret`,
    action: {
      label: 'Se opdateringer',
      onClick: () => console.log('View data updates'),
    },
  }),

  systemError: (operation: string, error: string): NotificationData => ({
    title: `Fejl i ${operation}`,
    description: error,
    action: {
      label: 'Prøv igen',
      onClick: () => console.log('Retry operation'),
    },
  }),

  dataUpdated: (dataType: string, updateTime: string): NotificationData => ({
    title: `${dataType} data opdateret`,
    description: `Nye data tilgængelige (opdateret ${updateTime})`,
    action: {
      label: 'Opdater visning',
      onClick: () => console.log('Refresh data view'),
    },
  }),
};

// Notification functions for data platform
export const showDataNotification = {
  success: (type: NotificationType, data: NotificationData) => {
    const icon = getNotificationIcon(type);
    toast.success(data.title, {
      description: data.description,
      icon: icon,
      action: data.action
        ? {
            label: data.action.label,
            onClick: data.action.onClick,
          }
        : undefined,
      duration: 5000,
    });
  },

  error: (type: NotificationType, data: NotificationData) => {
    const icon = getNotificationIcon(type);
    toast.error(data.title, {
      description: data.description,
      icon: icon,
      action: data.action
        ? {
            label: data.action.label,
            onClick: data.action.onClick,
          }
        : undefined,
      duration: 8000,
    });
  },

  warning: (type: NotificationType, data: NotificationData) => {
    const icon = getNotificationIcon(type);
    toast.warning(data.title, {
      description: data.description,
      icon: icon,
      action: data.action
        ? {
            label: data.action.label,
            onClick: data.action.onClick,
          }
        : undefined,
      duration: 6000,
    });
  },

  info: (type: NotificationType, data: NotificationData) => {
    const icon = getNotificationIcon(type);
    toast.info(data.title, {
      description: data.description,
      icon: icon,
      action: data.action
        ? {
            label: data.action.label,
            onClick: data.action.onClick,
          }
        : undefined,
      duration: 4000,
    });
  },

  promise: <T,>(
    promise: Promise<T>,
    messages: {
      loading: string;
      success: (data: T) => string;
      error: (error: Error) => string;
    },
    type: NotificationType
  ) => {
    const icon = getNotificationIcon(type);
    return toast.promise(promise, {
      loading: messages.loading,
      success: messages.success,
      error: messages.error,
      icon: icon,
    });
  },
};

// Get appropriate icon for notification type
function getNotificationIcon(type: NotificationType) {
  switch (type) {
    case 'analysis_complete':
      return <TrendingUp className="h-4 w-4" />;
    case 'export_ready':
      return <CheckCircle className="h-4 w-4" />;
    case 'data_sync':
      return <CheckCircle className="h-4 w-4" />;
    case 'system_error':
      return <AlertTriangle className="h-4 w-4" />;
    case 'data_updated':
      return <Info className="h-4 w-4" />;
    default:
      return <Info className="h-4 w-4" />;
  }
}

// Demo component to test realistic data platform notifications
export function NotificationDemo() {
  const handleAnalysisComplete = () => {
    showDataNotification.success(
      'analysis_complete',
      dataNotifications.analysisComplete('Pesticid belastning', 2847)
    );
  };

  const handleExportReady = () => {
    showDataNotification.success(
      'export_ready',
      dataNotifications.exportReady('Virksomheder CSV', 2847, '1.2 MB')
    );
  };

  const handleDataSync = () => {
    showDataNotification.info(
      'data_sync',
      dataNotifications.dataSync('CHR registret', 156234)
    );
  };

  const handleSystemError = () => {
    showDataNotification.error(
      'system_error',
      dataNotifications.systemError(
        'data indlæsning',
        'Kunne ikke forbinde til database - prøv igen senere'
      )
    );
  };

  const handleDataUpdated = () => {
    showDataNotification.info(
      'data_updated',
      dataNotifications.dataUpdated('Pesticid data', '2 timer siden')
    );
  };

  const handleAnalysisPromise = () => {
    const analysisPromise = new Promise<{ results: number }>(
      (resolve, reject) => {
        setTimeout(() => {
          if (Math.random() > 0.2) {
            resolve({ results: 2847 });
          } else {
            reject(new Error('Database forbindelse fejlede'));
          }
        }, 3000);
      }
    );

    showDataNotification.promise(
      analysisPromise,
      {
        loading: 'Behandler data...',
        success: (data) => `Analyse færdig - ${data.results} resultater fundet`,
        error: (error) => `Fejl: ${error.message}`,
      },
      'analysis_complete'
    );
  };

  return (
    <div className="space-y-4">
      <h3 className="text-lg font-semibold">Data Platform Notifikationer</h3>
      <div className="grid grid-cols-2 gap-3">
        <Button onClick={handleAnalysisComplete} variant="default">
          <TrendingUp className="mr-2 h-4 w-4" />
          Analyse Færdig
        </Button>

        <Button onClick={handleExportReady} variant="default">
          <CheckCircle className="mr-2 h-4 w-4" />
          Eksport Klar
        </Button>

        <Button onClick={handleDataSync} variant="secondary">
          <CheckCircle className="mr-2 h-4 w-4" />
          Data Synkroniseret
        </Button>

        <Button onClick={handleSystemError} variant="destructive">
          <AlertTriangle className="mr-2 h-4 w-4" />
          System Fejl
        </Button>

        <Button onClick={handleDataUpdated} variant="outline">
          <Info className="mr-2 h-4 w-4" />
          Data Opdateret
        </Button>

        <Button onClick={handleAnalysisPromise} variant="default">
          <TrendingUp className="mr-2 h-4 w-4" />
          Kør Analyse (Promise)
        </Button>
      </div>
    </div>
  );
}
