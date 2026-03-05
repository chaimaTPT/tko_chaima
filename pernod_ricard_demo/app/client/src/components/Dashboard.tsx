import React, { useEffect, useState } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell,
} from 'recharts';
import {
  Users, CheckCircle, AlertTriangle, TrendingUp,
  Clock, ShieldCheck, RefreshCw, Copy, Layers,
} from 'lucide-react';

interface DashboardData {
  totalSuppliers: number;
  approvedCount: number;
  escalatedCount: number;
  byCategory: { category: string; count: number }[];
  byPrefecture: { prefecture: string; count: number }[];
  avgReliability: number;
  avgLeadTime: number;
  certCoverage: number;
  riskBreakdown: { risk_level: string; count: number }[];
  duplicatesDetected: number;
}

const CHART_COLORS = ['#1B2A4A', '#C5A55A', '#B83232', '#405582', '#8c99b5', '#dccf7e', '#d04848', '#334468', '#6a5625', '#26334e'];
const RISK_COLORS: Record<string, string> = { HIGH: '#B83232', MEDIUM: '#C5A55A', LOW: '#22c55e' };

function KPICard({ icon: Icon, label, value, suffix, color }: {
  icon: React.ElementType; label: string; value: string | number; suffix?: string; color: string;
}) {
  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-5 hover:shadow-md transition-shadow">
      <div className="flex items-start justify-between">
        <div>
          <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">{label}</p>
          <p className="mt-2 text-2xl font-bold text-navy-800">
            {value}{suffix && <span className="text-base font-medium text-gray-400 ml-1">{suffix}</span>}
          </p>
        </div>
        <div className={`p-2.5 rounded-lg ${color}`}>
          <Icon size={20} className="text-white" />
        </div>
      </div>
    </div>
  );
}

export default function Dashboard() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch('/api/dashboard');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setData(json);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchData(); }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-20">
        <RefreshCw className="animate-spin text-navy-800 mr-3" size={24} />
        <span className="text-gray-600 font-medium">Loading dashboard...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-xl p-6 text-center">
        <AlertTriangle className="mx-auto text-warmred-500 mb-2" size={32} />
        <p className="text-red-700 font-medium">Failed to load dashboard</p>
        <p className="text-red-500 text-sm mt-1">{error}</p>
        <button onClick={fetchData} className="mt-4 px-4 py-2 bg-navy-800 text-white rounded-lg text-sm hover:bg-navy-700 transition-colors">
          Retry
        </button>
      </div>
    );
  }

  if (!data) return null;

  const approvedPct = data.totalSuppliers > 0
    ? Math.round((data.approvedCount / data.totalSuppliers) * 100)
    : 0;

  return (
    <div className="space-y-6">
      {/* KPI Row */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <KPICard icon={Users} label="Total Suppliers" value={data.totalSuppliers} color="bg-navy-800" />
        <KPICard icon={CheckCircle} label="Auto-Approved" value={data.approvedCount} suffix={`(${approvedPct}%)`} color="bg-green-600" />
        <KPICard icon={AlertTriangle} label="Escalated (Pending)" value={data.escalatedCount} color="bg-warmred-500" />
        <KPICard icon={ShieldCheck} label="Certification Coverage" value={data.certCoverage} suffix="%" color="bg-gold-400" />
      </div>

      {/* Secondary KPIs */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <KPICard icon={TrendingUp} label="Avg. Reliability Score" value={data.avgReliability} color="bg-navy-600" />
        <KPICard icon={Clock} label="Avg. Lead Time" value={data.avgLeadTime} suffix="days" color="bg-navy-500" />
        <KPICard icon={Copy} label="Duplicates Detected" value={data.duplicatesDetected} suffix="pairs" color="bg-gold-500" />
      </div>

      {/* Charts Row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Suppliers by Category */}
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
          <h3 className="text-sm font-semibold text-navy-800 uppercase tracking-wider mb-4">
            Suppliers by Category
          </h3>
          <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={data.byCategory} margin={{ top: 5, right: 20, left: 0, bottom: 60 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                <XAxis
                  dataKey="category"
                  tick={{ fontSize: 11, fill: '#666' }}
                  angle={-35}
                  textAnchor="end"
                  interval={0}
                />
                <YAxis tick={{ fontSize: 11, fill: '#666' }} />
                <Tooltip
                  contentStyle={{ borderRadius: '8px', border: '1px solid #e5e7eb', fontSize: '13px' }}
                />
                <Bar dataKey="count" fill="#1B2A4A" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Suppliers by Prefecture */}
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
          <h3 className="text-sm font-semibold text-navy-800 uppercase tracking-wider mb-4">
            Suppliers by Prefecture
          </h3>
          <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={data.byPrefecture} layout="vertical" margin={{ top: 5, right: 20, left: 80, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                <XAxis type="number" tick={{ fontSize: 11, fill: '#666' }} />
                <YAxis
                  type="category"
                  dataKey="prefecture"
                  tick={{ fontSize: 11, fill: '#666' }}
                  width={75}
                />
                <Tooltip
                  contentStyle={{ borderRadius: '8px', border: '1px solid #e5e7eb', fontSize: '13px' }}
                />
                <Bar dataKey="count" fill="#C5A55A" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* Risk Breakdown */}
      {data.riskBreakdown.length > 0 && (
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
          <h3 className="text-sm font-semibold text-navy-800 uppercase tracking-wider mb-4">
            Escalation Risk Breakdown
          </h3>
          <div className="flex items-center gap-6">
            <div className="h-48 w-48">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={data.riskBreakdown}
                    dataKey="count"
                    nameKey="risk_level"
                    cx="50%"
                    cy="50%"
                    outerRadius={80}
                    innerRadius={40}
                    paddingAngle={3}
                  >
                    {data.riskBreakdown.map((entry) => (
                      <Cell key={entry.risk_level} fill={RISK_COLORS[entry.risk_level] || '#8c99b5'} />
                    ))}
                  </Pie>
                  <Tooltip contentStyle={{ borderRadius: '8px', border: '1px solid #e5e7eb', fontSize: '13px' }} />
                </PieChart>
              </ResponsiveContainer>
            </div>
            <div className="flex flex-col gap-3">
              {data.riskBreakdown.map((entry) => (
                <div key={entry.risk_level} className="flex items-center gap-3">
                  <div
                    className="w-3 h-3 rounded-full"
                    style={{ backgroundColor: RISK_COLORS[entry.risk_level] || '#8c99b5' }}
                  />
                  <span className="text-sm font-medium text-gray-700 w-20">{entry.risk_level}</span>
                  <span className="text-sm text-gray-500">{entry.count} suppliers</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Architecture Overview */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
        <h3 className="text-sm font-semibold text-navy-800 uppercase tracking-wider mb-4">
          Platform Architecture
        </h3>
        <div className="flex items-center gap-2 overflow-x-auto pb-2">
          {[
            { label: 'Raw Data', desc: 'Bilingual CSV/JSON', color: 'bg-gray-100 border-gray-300' },
            { label: 'AI SQL Cleaning', desc: 'ai_query() standardization', color: 'bg-navy-50 border-navy-200' },
            { label: 'Agent Processing', desc: 'Compliance + anomaly detection', color: 'bg-gold-50 border-gold-200' },
            { label: 'Gold Tables', desc: 'Validated + escalation queue', color: 'bg-green-50 border-green-200' },
            { label: 'AI Surfaces', desc: 'KA, Genie, Supervisor, App', color: 'bg-navy-50 border-navy-200' },
          ].map((item, i) => (
            <React.Fragment key={i}>
              {i > 0 && <div className="flex-shrink-0 w-8 h-0.5 bg-gold-400" />}
              <div className={`flex-shrink-0 border rounded-lg px-4 py-3 text-center min-w-[150px] ${item.color}`}>
                <p className="text-sm font-medium text-navy-800">{item.label}</p>
                <p className="text-xs text-gray-500 mt-0.5">{item.desc}</p>
              </div>
            </React.Fragment>
          ))}
        </div>
      </div>
    </div>
  );
}
