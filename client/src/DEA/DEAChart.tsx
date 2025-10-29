import React from 'react';
import dynamic from 'next/dynamic';
import { ApexOptions } from 'apexcharts';
import * as antd from 'antd';
import { FilePdfOutlined } from '@ant-design/icons';

const Chart = dynamic(() => import('react-apexcharts'), { ssr: false });

interface DEAChartProps {
  obligations: any[];
  onExport: () => void;
}

const DEAChart: React.FC<DEAChartProps> = ({ obligations, onExport }) => {
  const prepareChartData = (obligations: any[]) => {
    const categories = ['Produit Attribution', 'Droit Établissement', 'Taxe Superficiaire', 'Redevance'];
    const dueData = [0, 0, 0, 0];
    const paidData = [0, 0, 0, 0];
    
    obligations.forEach(obligation => {
      const paid = obligation.payments?.reduce((sum: number, p: any) => sum + (p.amount || 0), 0) || 0;
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

  const chartOptions: ApexOptions = {
    chart: {
      type: 'bar',
      height: 350,
      stacked: true,
      toolbar: { show: false }
    },
    plotOptions: {
      bar: {
        horizontal: false,
        borderRadius: 4,
        dataLabels: { total: { enabled: true } }
      }
    },
    colors: ['#008FFB', '#00E396'],
    dataLabels: { enabled: false }
  };

  const chartData = prepareChartData(obligations);

  return (
    <div className="chart-section">
      <antd.Card 
        title="Répartition des Paiements par Type" 
        extra={
          <antd.Button type="text" icon={<FilePdfOutlined />} onClick={onExport}>
            Exporter
          </antd.Button>
        }
        variant="borderless"
      >
        <Chart 
          options={{ ...chartOptions, xaxis: { categories: chartData.categories } }} 
          series={chartData.series} 
          type="bar" 
          height={350} 
        />
      </antd.Card>
    </div>
  );
};

export default DEAChart;