import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import axios from 'axios';
import dynamic from 'next/dynamic';
import { ApexOptions } from 'apexcharts';
import {
  DollarOutlined,
  CalendarOutlined,
  FilePdfOutlined,
  MoreOutlined,
  SearchOutlined,
  MinusOutlined,
  PlusOutlined,
  LoadingOutlined
} from '@ant-design/icons';
import Navbar from '../navbar/Navbar';
import Sidebar from '../sidebar/Sidebar';
import { useAuthReady } from '@/src/hooks/useAuthReady';
import { useViewNavigator } from '../../src/hooks/useViewNavigator';
import styles from './dea.module.css';
import { DashboardStats, ObligationDto, PaymentDto } from '../../src/dto/payment.dto';
import dayjs, { Dayjs } from 'dayjs';
import router from 'next/router';

// Regular Ant Design imports
import {
  Button,
  Card,
  Divider,
  Input,
  Tag,
  Progress,
  Dropdown,
  Statistic,
  Table,
  Select,
  Menu,
  DatePicker,
  Typography,
  Spin,
  Alert
} from 'antd';

const { Title, Text } = Typography;
const { RangePicker } = DatePicker;
const { Option } = Select;

// Only dynamically import heavy chart library
const Chart = dynamic(() => import('react-apexcharts'), {
  ssr: false,
  loading: () => <div>Loading chart...</div>
});

// Data readiness interface
interface DataReadiness {
  obligations: boolean;
  stats: boolean;
  userData: boolean;
  viewData: boolean;
}

interface GroupedObligations {
  [key: string]: ObligationDto[];
}

// Custom hook for data readiness monitoring - MOVED TO TOP LEVEL
const useDataReadiness = () => {
  const [dataReadiness, setDataReadiness] = useState<DataReadiness>({
    obligations: false,
    stats: false,
    userData: false,
    viewData: false
  });

  const [isDataStable, setIsDataStable] = useState(false);
  const [renderAttempts, setRenderAttempts] = useState(0);
  const dataCheckIntervalRef = useRef<NodeJS.Timeout | null>(null);
  const maxRenderAttempts = 100;

  const resetMonitoring = useCallback(() => {
    if (dataCheckIntervalRef.current) {
      clearInterval(dataCheckIntervalRef.current);
      dataCheckIntervalRef.current = null;
    }

    setDataReadiness({
      obligations: false,
      stats: false,
      userData: false,
      viewData: false
    });

    setRenderAttempts(0);
    setIsDataStable(false);
  }, []);

  const checkDataReadiness = useCallback((currentState: {
    obligations: ObligationDto[];
    stats: DashboardStats | null;
    currentView: string;
  }) => {
    const newReadiness: DataReadiness = {
      obligations: Array.isArray(currentState.obligations) && currentState.obligations.length >= 0,
      stats: currentState.stats !== null && typeof currentState.stats === 'object',
      userData: true,
      viewData: !!currentState.currentView
    };

    return newReadiness;
  }, []);

  const isAllDataReady = useCallback((readiness: DataReadiness): boolean => {
    return Object.values(readiness).every(Boolean);
  }, []);

  const startDataMonitoring = useCallback((
    getCurrentState: () => {
      obligations: ObligationDto[];
      stats: DashboardStats | null;
      currentView: string;
    },
    onDataReady: () => void
  ) => {
    if (isDataStable) return;

    if (dataCheckIntervalRef.current) {
      clearInterval(dataCheckIntervalRef.current);
    }

    dataCheckIntervalRef.current = setInterval(() => {
      const currentState = getCurrentState();
      const currentReadiness = checkDataReadiness(currentState);
      setDataReadiness(currentReadiness);

      setRenderAttempts(prev => {
        const newAttempts = prev + 1;
        const allReady = isAllDataReady(currentReadiness);

        if (allReady || newAttempts >= maxRenderAttempts) {
          if (dataCheckIntervalRef.current) {
            clearInterval(dataCheckIntervalRef.current);
            dataCheckIntervalRef.current = null;
          }
          setIsDataStable(true);
          if (allReady) {
            onDataReady();
          }
        }

        return newAttempts;
      });
    }, 1);

    return () => {
      if (dataCheckIntervalRef.current) {
        clearInterval(dataCheckIntervalRef.current);
        dataCheckIntervalRef.current = null;
      }
    };
  }, [isDataStable, checkDataReadiness, isAllDataReady, maxRenderAttempts]);

  return {
    dataReadiness,
    isDataStable,
    renderAttempts,
    startDataMonitoring,
    resetMonitoring,
    setIsDataStable
  };
};

// Create a custom hook for antd message
const useAntdMessage = () => {
  const [messageApi, setMessageApi] = useState<any>(null);

  useEffect(() => {
    const loadMessage = async () => {
      const { default: antdMessage } = await import('antd/es/message');
      setMessageApi(antdMessage);
    };
    loadMessage();
  }, []);

  const showMessage = useCallback((type: 'success' | 'error' | 'info', content: string) => {
    if (messageApi) {
      messageApi[type](content);
    }
  }, [messageApi]);

  return showMessage;
};

type Obligation = ObligationDto;
type Payment = PaymentDto;

const DEADashboard: React.FC = () => {
  const apiURL = process.env.NEXT_PUBLIC_API_URL;
  const isAuthReady = useAuthReady();

  // ALL HOOKS MUST BE CALLED AT THE TOP LEVEL, NEVER CONDITIONALLY
  const [loading, setLoading] = useState(true);
  const [obligations, setObligations] = useState<Obligation[]>([]);
  const [filteredObligations, setFilteredObligations] = useState<Obligation[]>([]);
  const { currentView, navigateTo } = useViewNavigator('Payments');
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [timeRange, setTimeRange] = useState<'week' | 'month' | 'year'>('month');
  const [searchTerm, setSearchTerm] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [permis_id, setpermisid] = useState(0);
  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());

  // Data readiness monitoring - MUST BE CALLED BEFORE ANY CONDITIONAL RETURNS
  const {
    dataReadiness,
    isDataStable,
    renderAttempts,
    startDataMonitoring,
    resetMonitoring,
    setIsDataStable
  } = useDataReadiness();

  const showMessage = useAntdMessage();

  // Refs for stable data storage
  const obligationsRef = useRef<Obligation[]>([]);
  const statsRef = useRef<DashboardStats | null>(null);
  const currentViewRef = useRef<string>('');

  // Safe data access functions with fallbacks - MOVED BEFORE CONDITIONAL RETURNS
  const getSafeObligations = useCallback((): Obligation[] => obligations, [obligations]);

  const getSafeStats = useCallback((): DashboardStats => {
    return dataReadiness.stats && stats ? stats : {
      totalDue: 0,
      totalPaid: 0,
      overdueAmount: 0,
      pendingCount: 0,
      totalObligations: 0,
      paidObligations: 0,
      overdueObligations: 0
    };
  }, [dataReadiness.stats, stats]);

  // Update refs when data changes
  useEffect(() => {
    obligationsRef.current = obligations;
    statsRef.current = stats;
    currentViewRef.current = currentView;
  }, [obligations, stats, currentView]);

  // Main data fetching effect
  useEffect(() => {
    if (!isAuthReady) {
      return;
    }

    const fetchData = async () => {
      try {
        resetMonitoring();
        setLoading(true);
        setError(null);

        const [obligationsResponse, statsResponse] = await Promise.all([
          axios.get(`${apiURL}/payments/obligations`, { withCredentials: true }),
          axios.get(`${apiURL}/payments/stats`, { withCredentials: true })
        ]);

        const obligationsData = obligationsResponse.data.map((ob: any) => ({
          ...ob,
          payments: ob.payments || [],
          permis: ob.permis || { code_permis: 'N/A', detenteur: null, id: 0 }
        }));

        console.log('Fetched obligations:', obligationsResponse.data);

        if (obligationsResponse.data.length > 0 && obligationsResponse.data[0].permis) {
          setpermisid(obligationsResponse.data[0].permis.id);
        }

        setObligations(obligationsData);
        setFilteredObligations(obligationsData);
        setStats(statsResponse.data);

      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : 'Failed to load data';
        setError(errorMessage);
        console.error('Error fetching data:', err);
        showMessage('error', errorMessage);
      } finally {
        setIsDataStable(true);
        setLoading(false);
      }
    };

    fetchData();
  }, [apiURL, isAuthReady, resetMonitoring, showMessage, setIsDataStable]);

  // Start data monitoring when component mounts and data changes
  useEffect(() => {
    if (!isAuthReady || isDataStable) {
      return;
    }

    const cleanup = startDataMonitoring(
      () => ({
        obligations: obligationsRef.current,
        stats: statsRef.current,
        currentView: currentViewRef.current
      }),
      () => {
        console.log('All data is ready for rendering');
      }
    );

    return cleanup;
  }, [isAuthReady, isDataStable, startDataMonitoring]);

  // Group obligations by permit with safe data access
  const groupObligationsByPermis = useCallback((source: Obligation[]): GroupedObligations => {
    return source.reduce((acc, obligation) => {
      const permisId = obligation.permis?.id?.toString() || 'unknown';
      if (!acc[permisId]) {
        acc[permisId] = [];
      }
      acc[permisId].push(obligation);
      return acc;
    }, {} as GroupedObligations);
  }, []);

  const groupedObligations = useMemo(
    () => groupObligationsByPermis(filteredObligations),
    [filteredObligations, groupObligationsByPermis]
  );

  // Create a flattened data source for the table with safe data
  const tableDataSource = useMemo(() => {
    const flattened = Object.entries(groupedObligations)
      .flatMap(([permisId, permisObligations]) => {
        const firstObligation = permisObligations[0];
        const isExpanded = expandedRows.has(permisId);

        return isExpanded ? permisObligations : [firstObligation];
      })
      .filter((item): item is Obligation => Boolean(item));

    return flattened.length > 0 ? flattened : filteredObligations;
  }, [groupedObligations, expandedRows, filteredObligations]);

  // Rest of your component functions...
  const toggleRowExpansion = (permisId: string) => {
    const newExpandedRows = new Set(expandedRows);
    if (newExpandedRows.has(permisId)) {
      newExpandedRows.delete(permisId);
    } else {
      newExpandedRows.add(permisId);
    }
    setExpandedRows(newExpandedRows);
  };

  const handleGoToPayments = (permisId: number | undefined) => {
    if (permisId && permisId > 0) {
      window.open(`/DEA/obligations?permisId=${permisId}`, "_blank");
    }
  };

  const handleFilterChange = (value: 'week' | 'month' | 'year') => {
    setTimeRange(value);
    const now = new Date();
    let startDate: Date;

    switch (value) {
      case 'week':
        startDate = new Date(now.setDate(now.getDate() - 7));
        break;
      case 'month':
        startDate = new Date(now.setMonth(now.getMonth() - 1));
        break;
      case 'year':
        startDate = new Date(now.setFullYear(now.getFullYear() - 1));
        break;
      default:
        return;
    }

    const filtered = getSafeObligations().filter(ob => {
      const dueDate = new Date(ob.dueDate || '');
      return dueDate >= startDate;
    });

    setFilteredObligations(filtered);
  };

  const handleGenerateReceipt = async (paymentId: number) => {
    try {
      setLoading(true);
      const response = await axios.post(`${apiURL}/payments/generate-receipt/${paymentId}`);

      if (response.data.pdfUrl) {
        window.open(response.data.pdfUrl, '_blank');
      } else {
        showMessage('error', 'Failed to generate receipt');
      }
    } catch (error) {
      console.error('Receipt generation error:', error);
      showMessage('error', 'Failed to generate receipt');
    } finally {
      setLoading(false);
    }
  };

  const handleExport = async () => {
    try {
      const XLSX = await import('xlsx');
      const FileSaver = await import('file-saver');

      const worksheetData = filteredObligations.map((ob) => ({
        Référence: ob.permis?.code_permis || '',
        Détenteur: ob.permis?.detenteur?.nom_societeFR || '',
        'Type de droit': ob.typePaiement?.libelle || '',
        Montant: ob.amount || 0,
        Échéance: ob.dueDate ? new Date(ob.dueDate).toLocaleDateString() : '',
        Statut: ob.status || ''
      }));

      const worksheet = XLSX.utils.json_to_sheet(worksheetData);
      const workbook = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(workbook, worksheet, 'Obligations');

      const excelBuffer = XLSX.write(workbook, {
        bookType: 'xlsx',
        type: 'array'
      });

      const blob = new Blob([excelBuffer], {
        type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
      });

      FileSaver.default.saveAs(
        blob,
        `obligations_export_${new Date().toISOString().slice(0, 10)}.xlsx`
      );

      showMessage('success', 'Export successful!');
    } catch (err) {
      console.error('Export error:', err);
      showMessage('error', 'Export failed');
    }
  };

  const prepareChartData = (obligations: Obligation[]) => {
    const categories = ['Produit Attribution', 'Droit Établissement', 'Taxe Superficiaire', 'Redevance'];
    const dueData = [0, 0, 0, 0];
    const paidData = [0, 0, 0, 0];

    obligations.forEach(obligation => {
      const paid = obligation.payments?.reduce((sum, p) => sum + (p.amount || 0), 0) || 0;
      const type = obligation.typePaiement?.libelle || '';

      if (type.includes('Produit')) {
        dueData[0] += obligation.amount || 0;
        paidData[0] += paid;
      } else if (type.includes('Droit')) {
        dueData[1] += obligation.amount || 0;
        paidData[1] += paid;
      } else if (type.includes('Taxe')) {
        dueData[2] += obligation.amount || 0;
        paidData[2] += paid;
      } else {
        dueData[3] += obligation.amount || 0;
        paidData[3] += paid;
      }
    });

    return {
      series: [
        { name: 'Dû', data: dueData },
        { name: 'Payé', data: paidData }
      ],
      categories
    };
  };

  const handleSearch = (e: React.ChangeEvent<HTMLInputElement>) => {
    const term = e.target.value.toLowerCase();
    setSearchTerm(term);

    if (term === '') {
      setFilteredObligations(getSafeObligations());
    } else {
      const filtered = getSafeObligations().filter(obligation => {
        const permisCode = obligation.permis?.code_permis?.toLowerCase() || '';
        const detenteurName = obligation.permis?.detenteur?.nom_societeFR?.toLowerCase() || '';
        const typeLibelle = obligation.typePaiement?.libelle?.toLowerCase() || '';

        return permisCode.includes(term) ||
               detenteurName.includes(term) ||
               typeLibelle.includes(term);
      });
      setFilteredObligations(filtered);
    }
  };

  // Loading state while data is not ready - CONDITIONAL RETURN MUST BE AFTER ALL HOOKS
  if (loading) {
    return (
      <div className={styles.appContainer}>
        <Navbar />
        <div className={styles.appContent}>
          <Sidebar currentView={currentView} navigateTo={navigateTo} />
          <main className={styles.mainContent}>
            <div className={styles.dataLoadingContainer}>
              <Spin
                indicator={<LoadingOutlined style={{ fontSize: 48 }} spin />}
                size="large"
              />
              <div className={styles.dataLoadingContent}>
                <Title level={3}>Chargement des donnees financieres...</Title>
                <div className={styles.dataReadinessStatus}>
                  <div className={dataReadiness.obligations ? styles.ready : styles.pending}>
                    Obligations: {dataReadiness.obligations ? 'OK' : '...'}
                  </div>
                  <div className={dataReadiness.stats ? styles.ready : styles.pending}>
                    Statistiques: {dataReadiness.stats ? 'OK' : '...'}
                  </div>
                  <div className={dataReadiness.viewData ? styles.ready : styles.pending}>
                    Interface: {dataReadiness.viewData ? 'OK' : '...'}
                  </div>
                </div>
                {renderAttempts >= 100 && (
                  <Alert
                    message="Donnees incompletes - Affichage en mode limite"
                    type="warning"
                    showIcon
                  />
                )}
              </div>
            </div>
          </main>
        </div>
      </div>
    );
  }

  // Error state - ANOTHER CONDITIONAL RETURN
  if (error && getSafeObligations().length === 0) {
    return (
      <div className={styles.appContainer}>
        <Navbar />
        <div className={styles.appContent}>
          <Sidebar currentView={currentView} navigateTo={navigateTo} />
          <main className={styles.mainContent}>
            <Alert
              message="Erreur de chargement"
              description={error}
              type="error"
              showIcon
              action={
                <Button size="small" onClick={() => window.location.reload()}>
                  Réessayer
                </Button>
              }
            />
          </main>
        </div>
      </div>
    );
  }

  // Rest of your component (statusColors, paymentMethods, columns, etc.)...
  const statusColors: Record<string, string> = {
    'Payé': 'green',
    'En retard': 'red',
    'Partiellement payé': 'orange',
    'A payer': 'blue'
  };

  const paymentMethods: Record<string, string> = {
    'Virement': 'blue',
    'Chèque': 'purple',
    'Espèces': 'gold'
  };

  const columns = [
    {
      title: '',
      key: 'expand',
      width: 50,
      render: (_: any, record: Obligation) => {
        const permisId = record.permis?.id?.toString() || 'unknown';
        const isExpanded = expandedRows.has(permisId);
        return (
          <Button
            type="text"
            icon={isExpanded ? <MinusOutlined /> : <PlusOutlined />}
            onClick={() => toggleRowExpansion(permisId)}
            size="small"
          />
        );
      },
    },
  {
    title: 'Référence',
    dataIndex: ['permis', 'code_permis'],
    key: 'reference',
    render: (code: string, record: Obligation) => (
      <div>
        <Text strong>{code || 'N/A'}</Text>
        <br />
        <Text type="secondary">
          {record.permis?.detenteur?.nom_societeFR || 'Détenteur non spécifié'}
        </Text>
      </div>
    ),
  },
  {
    title: 'Nombre d\'obligations',
    key: 'count',
    render: (_: any, record: Obligation) => {
      const permisId = record.permis?.id?.toString() || 'unknown';
      const group = groupedObligations[permisId];
      return group ? group.length : 0;
    },
  },
  {
    title: 'Montant Total',
    key: 'totalAmount',
    render: (_: any, record: Obligation) => {
      const permisId = record.permis?.id?.toString() || 'unknown';
      const group = groupedObligations[permisId];
      const total = group ? group.reduce((sum, ob) => sum + (ob.amount || 0), 0) : 0;
      return <Text strong>{total.toLocaleString()} DZD</Text>;
    },
  },
  {
      title: 'Type de droit',
      dataIndex: ['typePaiement', 'libelle'],
      key: 'type',
      render: (libelle: string, record: Obligation) => (
        <div>
          <Text strong>{libelle || 'N/A'}</Text>
          <br />
          <Text type="secondary">
            NIF: {record.permis?.detenteur?.registreCommerce?.nif || 'N/A'}
          </Text>
        </div>
      )
    },
    {
      title: 'Montant',
      dataIndex: 'amount',
      key: 'amount',
      render: (amount: number, record: Obligation) => {
        const payments = record.payments || [];
        const paid = payments.reduce((sum, p) => sum + (p.amount || 0), 0);
        const remaining = (amount || 0) - paid;
        const percent = amount ? Math.round((paid / amount) * 100) : 0;

        return (
          <div>
            <Text strong>{(amount || 0).toLocaleString()} DZD</Text>
            <br />
            <Progress
              percent={percent}
              size="small"
              status={remaining > 0 ? 'active' : 'success'}
            />
            <Text type="secondary">Payé: {paid.toLocaleString()} DZD</Text>
          </div>
        );
      },
      sorter: (a: Obligation, b: Obligation) => (a.amount || 0) - (b.amount || 0)
    },
    {
      title: 'Échéance',
      dataIndex: 'dueDate',
      key: 'dueDate',
      render: (date: string) => (
        <div>
          <CalendarOutlined style={{ marginRight: 8 }} />
          {date ? new Date(date).toLocaleDateString() : 'N/A'}
        </div>
      ),
      sorter: (a: Obligation, b: Obligation) =>
        new Date(a.dueDate || 0).getTime() - new Date(b.dueDate || 0).getTime()
    },
    {
      title: 'Statut',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => (
        <Tag color={statusColors[status] || 'default'}>
          {status?.toUpperCase() || 'INCONNU'}
        </Tag>
      )
    },
  {
    title: 'Actions',
    key: 'actions',
    render: (_: any, record: Obligation) => (
      <Dropdown
        overlay={
          <Menu>
            <Menu.Item
              key="1"
              icon={<FilePdfOutlined />}
              onClick={() => record.payments?.length > 0 && handleGenerateReceipt(record.payments[0].id)}
              disabled={!record.payments?.length}
            >
              Générer quittance
            </Menu.Item>
            <Menu.Item
              key="2"
              icon={<DollarOutlined />}
              onClick={() => handleGoToPayments(record.permis?.id)}
            >
              Gérer les paiements
            </Menu.Item>
          </Menu>
        }
        trigger={['click']}
      >
        <Button shape="circle" icon={<MoreOutlined />} />
      </Dropdown>
    )
  }
];

  const paymentHistoryColumns = [
    {
      title: 'Date',
      dataIndex: 'paymentDate',
      key: 'date',
      render: (date: string) => date ? new Date(date).toLocaleDateString() : 'N/A'
    },
    {
      title: 'Montant',
      dataIndex: 'amount',
      key: 'amount',
      render: (amount: number) => `${(amount || 0).toLocaleString()} DZD`
    },
    {
      title: 'Méthode',
      dataIndex: 'paymentMethod',
      key: 'method',
      render: (method: string) => (
        <Tag color={paymentMethods[method] || 'default'}>
          {method || 'Inconnu'}
        </Tag>
      )
    },
    {
      title: 'Quittance',
      dataIndex: 'receiptNumber',
      key: 'receipt'
    },
    {
      title: 'Action',
      key: 'action',
      render: (_: any, record: Payment) => (
        <Button
          type="link"
          icon={<FilePdfOutlined />}
          onClick={() => handleGenerateReceipt(record.id)}
          disabled={!record.id}
        />
      )
    }
  ];

  const chartOptions: ApexOptions = {
    chart: {
      type: 'bar',
      height: 350,
      stacked: true,
      toolbar: {
        show: false
      }
    },
    plotOptions: {
      bar: {
        horizontal: false,
        borderRadius: 4,
        dataLabels: {
          total: {
            enabled: true,
            style: {
              fontSize: '13px',
              fontWeight: 900
            }
          }
        }
      }
    },
    colors: ['#008FFB', '#00E396'],
    legend: {
      position: 'top',
      horizontalAlign: 'left',
      offsetX: 40
    },
    dataLabels: {
      enabled: false
    },
    tooltip: {
      y: {
        formatter: function (val: number) {
          return val.toLocaleString() + ' DZD';
        }
      }
    }
  };

  const chartData = prepareChartData(obligations);

  if (error) {
    return <div>Error: {error}</div>;
  }

  return (
    <div className={styles.appContainer}>
      <Navbar />
      <div className={styles.appContent}>
        <Sidebar currentView={currentView} navigateTo={navigateTo} />
        <main className={styles.mainContent}>
          <div className="dea-dashboard">
            <div className="dashboard-header">
              <Title level={2} style={{ marginBottom: 0 }}>
                Détail des Droits, Échéances et Redevance
              </Title>
              <div className="header-actions">
                <RangePicker
  style={{ marginRight: 16 }}
  onChange={(
    dates: [Dayjs | null, Dayjs | null] | null,
    dateStrings: [string, string]
  ) => {
    if (!dates || !dates[0] || !dates[1]) {
      setFilteredObligations(obligations);
      return;
    }

    const [start, end] = dates;
    const filtered = obligations.filter((ob) => {
      const dueDate = dayjs(ob.dueDate);
      return dueDate.isAfter(start) && dueDate.isBefore(end);
    });
    setFilteredObligations(filtered);
  }}
/>
                <Select
  defaultValue="month"
  style={{ width: 120 }}
  onChange={handleFilterChange}
>
  <Option value="week">Cette semaine</Option>
  <Option value="month">Ce mois</Option>
  <Option value="year">Cette année</Option>
</Select>
              </div>
            </div>

            <Divider />

            <div className="stats-row">
              <Card variant="borderless">
                <Statistic
                  title="Total Dû"
                  value={stats?.totalDue!}
                  precision={2}
                  valueStyle={{ color: '#3f8600' }}
                  prefix={<DollarOutlined />}
                  suffix="DZD"
                />
              </Card>
              <Card variant="borderless">
                <Statistic
                  title="Total Payé"
                  value={stats?.totalPaid!}
                  precision={2}
                  valueStyle={{ color: '#3f8600' }}
                  prefix={<DollarOutlined />}
                  suffix="DZD"
                />
              </Card>
              <Card variant="borderless">
                <Statistic
                  title="En Retard"
                  value={stats?.overdueAmount!}
                  precision={2}
                  valueStyle={{ color: '#cf1322' }}
                  prefix={<DollarOutlined />}
                  suffix="DZD"
                />
              </Card>
              <Card variant="borderless">
                <Statistic
                  title="Obligations"
                  value={`${stats?.paidObligations!}/${stats?.totalObligations!}`}
                  valueStyle={{ color: '#1890ff' }}
                  prefix={<CalendarOutlined />}
                  suffix={`payées (${stats?.overdueObligations!} en retard)`}
                />
              </Card>
            </div>

            <div className="chart-section">
              <Card
                title="Répartition des Paiements par Type"
                extra={
                  <Button type="text" icon={<FilePdfOutlined />} onClick={handleExport}>
                    Exporter
                  </Button>
                }
                variant="borderless"
              >
                <Chart
                  options={{ ...chartOptions, xaxis: { categories: chartData.categories } }}
                  series={chartData.series}
                  type="bar"
                  height={350}
                />
              </Card>
            </div>

            <div className="obligations-section">
              <Card
                title={`Liste des Obligations (${filteredObligations.length})`}
                extra={
                  <Input
                    placeholder="Rechercher..."
                    prefix={<SearchOutlined />}
                    style={{ width: 200 }}
                    onChange={handleSearch}
                    value={searchTerm}
                  />
                }
                variant="borderless"
              >
                <Table
  columns={columns}
  dataSource={tableDataSource}
  rowKey={(record) => `${record.permis?.id}-${record.id}`}
  loading={loading}
  expandable={{
    expandedRowRender: (record: Obligation) => (
      <div style={{ margin: 0 }}>
        <Title level={5} style={{ marginBottom: 16 }}>
          Historique des Paiements
        </Title>
        <Table
          columns={paymentHistoryColumns}
          dataSource={record.payments || []}
          rowKey="id"
          pagination={false}
          size="small"
        />
      </div>
    ),
    rowExpandable: (record: Obligation) => (record.payments?.length || 0) > 0
  }}
/>
              </Card>
            </div>

            <style jsx>{`
              .dea-dashboard {
                padding: 24px;
                background-color: #f5f7fa;
              }
              .dashboard-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 24px;
              }
              .stats-row {
                display: grid;
                grid-template-columns: repeat(4, 1fr);
                gap: 16px;
                margin-bottom: 24px;
              }
              .chart-section {
                margin-bottom: 24px;
              }
              @media (max-width: 1200px) {
                .stats-row {
                  grid-template-columns: repeat(2, 1fr);
                }
              }
              @media (max-width: 768px) {
                .stats-row {
                  grid-template-columns: 1fr;
                }
                .dashboard-header {
                  flex-direction: column;
                  align-items: flex-start;
                }
                .header-actions {
                  margin-top: 16px;
                  width: 100%;
                }
              }
            `}</style>
          </div>
        </main>
      </div>
    </div>
  );
};

export default DEADashboard;
