import React from 'react';
import * as antd from 'antd';
import {
  DollarOutlined,
  CalendarOutlined,
  FilePdfOutlined,
  MoreOutlined,
  SearchOutlined
} from '@ant-design/icons';

interface ObligationsTableProps {
  data: any[];
  loading: boolean;
  onGenerateReceipt: (id: number) => void;
  searchTerm: string;
  onSearch: (e: React.ChangeEvent<HTMLInputElement>) => void;
}

const ObligationsTable: React.FC<ObligationsTableProps> = ({
  data,
  loading,
  onGenerateReceipt,
  searchTerm,
  onSearch
}) => {
  const statusColors: Record<string, string> = {
    'Payé': 'green',
    'En retard': 'red',
    'Partiellement payé': 'orange',
    'A payer': 'blue'
  };

  const columns = [
    {
      title: 'Référence',
      dataIndex: ['permis', 'code_permis'],
      key: 'reference',
      render: (code: string, record: any) => (
        <div>
          <antd.Typography.Text strong>{code || 'N/A'}</antd.Typography.Text>
          <br />
          <antd.Typography.Text type="secondary">
            {record.permis?.detenteur?.nom_societeFR || 'Détenteur non spécifié'}
          </antd.Typography.Text>
        </div>
      )
    },
    {
      title: 'Type de droit',
      dataIndex: ['typePaiement', 'libelle'],
      key: 'type',
      render: (libelle: string, record: any) => (
        <div>
          <antd.Typography.Text strong>{libelle || 'N/A'}</antd.Typography.Text>
          <br />
          <antd.Typography.Text type="secondary">
            NIF: {record.permis?.detenteur?.registreCommerce?.nif || 'N/A'}
          </antd.Typography.Text>
        </div>
      )
    },
    {
      title: 'Montant',
      dataIndex: 'amount',
      key: 'amount',
      render: (amount: number, record: any) => {
        const payments = record.payments || [];
        const paid = payments.reduce((sum: number, p: any) => sum + (p.amount || 0), 0);
        const remaining = (amount || 0) - paid;
        const percent = amount ? Math.round((paid / amount) * 100) : 0;
        
        return (
          <div>
            <antd.Typography.Text strong>{(amount || 0).toLocaleString()} DZD</antd.Typography.Text>
            <br />
            <antd.Progress 
              percent={percent} 
              size="small" 
              status={remaining > 0 ? 'active' : 'success'}
            />
            <antd.Typography.Text type="secondary">Payé: {paid.toLocaleString()} DZD</antd.Typography.Text>
          </div>
        );
      }
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
      )
    },
    {
      title: 'Statut',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => (
        <antd.Tag color={statusColors[status] || 'default'}>
          {status?.toUpperCase() || 'INCONNU'}
        </antd.Tag>
      )
    },
    {
      title: 'Actions',
      key: 'actions',
      render: (_: any, record: any) => (
        <antd.Dropdown overlay={
          <antd.Menu>
            <antd.Menu.Item 
              key="1" 
              icon={<FilePdfOutlined />}
              onClick={() => record.payments?.length > 0 && onGenerateReceipt(record.payments[0].id)}
              disabled={!record.payments?.length}
            >
              Générer quittance
            </antd.Menu.Item>
          </antd.Menu>
        }>
          <antd.Button shape="circle" icon={<MoreOutlined />} />
        </antd.Dropdown>
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
        <antd.Tag color={method === 'Virement' ? 'blue' : method === 'Chèque' ? 'purple' : 'gold'}>
          {method || 'Inconnu'}
        </antd.Tag>
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
      render: (_: any, record: any) => (
        <antd.Button 
          type="link" 
          icon={<FilePdfOutlined />} 
          onClick={() => onGenerateReceipt(record.id)}
          disabled={!record.id}
        />
      )
    }
  ];

  return (
    <div className="obligations-section">
      <antd.Card
        title={`Liste des Obligations (${data.length})`}
        extra={
          <antd.Input
            placeholder="Rechercher..."
            prefix={<SearchOutlined />}
            style={{ width: 200 }}
            onChange={onSearch}
            value={searchTerm}
          />
        }
        variant="borderless"
      >
        <antd.Table
          columns={columns}
          dataSource={data}
          rowKey="id"
          loading={loading}
          expandable={{
            expandedRowRender: (record: any) => (
              <div style={{ margin: 0 }}>
                <antd.Typography.Title level={5} style={{ marginBottom: 16 }}>
                  Historique des Paiements
                </antd.Typography.Title>
                <antd.Table
                  columns={paymentHistoryColumns}
                  dataSource={record.payments || []}
                  rowKey="id"
                  pagination={false}
                  size="small"
                />
              </div>
            ),
            rowExpandable: (record: any) => (record.payments?.length || 0) > 0
          }}
        />
      </antd.Card>
    </div>
  );
};

export default ObligationsTable;