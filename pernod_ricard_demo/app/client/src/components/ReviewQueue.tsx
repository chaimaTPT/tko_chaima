import React, { useEffect, useState } from 'react';
import {
  RefreshCw, CheckCircle, XCircle, AlertTriangle,
  Shield, Filter, Loader2, ChevronDown, Search,
} from 'lucide-react';

interface Escalation {
  supplier_id: string;
  supplier_name: string;
  category: string;
  risk_level: string;
  issues: string;
  recommendation: string;
  review_status: string;
}

const RISK_COLORS: Record<string, { bg: string; text: string; border: string }> = {
  HIGH: { bg: 'bg-red-50', text: 'text-red-700', border: 'border-red-200' },
  MEDIUM: { bg: 'bg-yellow-50', text: 'text-yellow-700', border: 'border-yellow-200' },
  LOW: { bg: 'bg-green-50', text: 'text-green-700', border: 'border-green-200' },
};

function RiskBadge({ level }: { level: string }) {
  const colors = RISK_COLORS[level] || RISK_COLORS.LOW;
  return (
    <span className={`inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-semibold ${colors.bg} ${colors.text} border ${colors.border}`}>
      {level === 'HIGH' && <AlertTriangle size={11} />}
      {level === 'MEDIUM' && <Shield size={11} />}
      {level}
    </span>
  );
}

function parseIssues(issues: string): string[] {
  if (!issues) return [];
  try {
    const parsed = JSON.parse(issues);
    if (Array.isArray(parsed)) return parsed;
  } catch {
    // Not JSON, try splitting
  }
  // Handle string format like ["issue1", "issue2"]
  const match = issues.match(/\[([^\]]*)\]/);
  if (match) {
    return match[1].split(',').map(s => s.trim().replace(/^["']|["']$/g, ''));
  }
  return [issues];
}

export default function ReviewQueue() {
  const [escalations, setEscalations] = useState<Escalation[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [riskFilter, setRiskFilter] = useState<string>('ALL');
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [filterOpen, setFilterOpen] = useState(false);

  const fetchEscalations = async () => {
    setLoading(true);
    setError(null);
    try {
      const params = riskFilter !== 'ALL' ? `?risk_level=${riskFilter}` : '';
      const res = await fetch(`/api/escalations${params}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setEscalations(json);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleReview = async (supplierId: string, action: 'APPROVED' | 'REJECTED') => {
    setActionLoading(supplierId);
    try {
      const res = await fetch(`/api/escalations/${supplierId}/review`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action }),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      setEscalations((prev) =>
        prev.map((e) => (e.supplier_id === supplierId ? { ...e, review_status: action } : e))
      );
    } catch (err: any) {
      alert(`Error: ${err.message}`);
    } finally {
      setActionLoading(null);
    }
  };

  useEffect(() => { fetchEscalations(); }, [riskFilter]);

  const filtered = escalations.filter((e) => {
    if (!searchTerm) return true;
    const term = searchTerm.toLowerCase();
    return (
      e.supplier_name?.toLowerCase().includes(term) ||
      e.category?.toLowerCase().includes(term) ||
      e.supplier_id?.toLowerCase().includes(term) ||
      e.issues?.toLowerCase().includes(term)
    );
  });

  const counts = {
    ALL: escalations.length,
    HIGH: escalations.filter((e) => e.risk_level === 'HIGH').length,
    MEDIUM: escalations.filter((e) => e.risk_level === 'MEDIUM').length,
    LOW: escalations.filter((e) => e.risk_level === 'LOW').length,
  };

  return (
    <div className="space-y-4">
      {/* Controls Bar */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
        <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-3">
          <div>
            <h2 className="text-lg font-semibold text-navy-800">Supplier Review Queue</h2>
            <p className="text-sm text-gray-500">
              {filtered.length} escalated supplier{filtered.length !== 1 ? 's' : ''} requiring review
            </p>
          </div>
          <div className="flex items-center gap-2 w-full sm:w-auto">
            {/* Search */}
            <div className="relative flex-1 sm:flex-initial">
              <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
              <input
                type="text"
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                placeholder="Search suppliers..."
                className="w-full sm:w-56 pl-9 pr-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-gold-400 focus:border-transparent"
              />
            </div>

            {/* Risk Filter */}
            <div className="relative">
              <button
                onClick={() => setFilterOpen(!filterOpen)}
                className="inline-flex items-center gap-2 px-3 py-2 text-sm border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
              >
                <Filter size={14} />
                {riskFilter === 'ALL' ? 'All Risk' : riskFilter}
                <ChevronDown size={14} />
              </button>
              {filterOpen && (
                <div className="absolute right-0 top-full mt-1 bg-white border border-gray-200 rounded-lg shadow-lg z-10 py-1 w-40">
                  {['ALL', 'HIGH', 'MEDIUM', 'LOW'].map((level) => (
                    <button
                      key={level}
                      onClick={() => { setRiskFilter(level); setFilterOpen(false); }}
                      className={`w-full text-left px-4 py-2 text-sm hover:bg-gray-50 flex items-center justify-between ${
                        riskFilter === level ? 'text-navy-800 font-medium' : 'text-gray-600'
                      }`}
                    >
                      <span>{level === 'ALL' ? 'All Levels' : level}</span>
                      <span className="text-xs text-gray-400">{counts[level as keyof typeof counts]}</span>
                    </button>
                  ))}
                </div>
              )}
            </div>

            <button
              onClick={fetchEscalations}
              disabled={loading}
              className="p-2 rounded-lg border border-gray-200 text-gray-500 hover:bg-gray-50 transition-colors disabled:opacity-50"
              title="Refresh"
            >
              <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
            </button>
          </div>
        </div>
      </div>

      {/* Table */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden">
        {error ? (
          <div className="p-8 text-center">
            <AlertTriangle className="mx-auto text-warmred-500 mb-2" size={28} />
            <p className="text-sm text-red-600 font-medium">{error}</p>
            <button onClick={fetchEscalations} className="mt-3 text-sm text-navy-700 underline">
              Retry
            </button>
          </div>
        ) : loading ? (
          <div className="p-8 text-center text-gray-500">
            <Loader2 className="animate-spin mx-auto mb-2" size={24} />
            Loading escalations...
          </div>
        ) : filtered.length === 0 ? (
          <div className="p-8 text-center text-gray-400">
            <Shield className="mx-auto mb-2" size={32} />
            <p className="font-medium">No escalations found</p>
            <p className="text-sm mt-1">
              {searchTerm ? 'Try a different search term.' : 'All suppliers are in good standing.'}
            </p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 text-left">
                  <th className="px-5 py-3 font-medium text-gray-500">Supplier ID</th>
                  <th className="px-5 py-3 font-medium text-gray-500">Supplier</th>
                  <th className="px-5 py-3 font-medium text-gray-500">Category</th>
                  <th className="px-5 py-3 font-medium text-gray-500">Risk</th>
                  <th className="px-5 py-3 font-medium text-gray-500">Issues</th>
                  <th className="px-5 py-3 font-medium text-gray-500 text-right">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {filtered.slice(0, 50).map((esc) => {
                  const issues = parseIssues(esc.issues);
                  return (
                    <tr key={esc.supplier_id} className={`hover:bg-gray-50 transition-colors ${
                      esc.review_status === 'APPROVED' ? 'bg-green-50/50' :
                      esc.review_status === 'REJECTED' ? 'bg-red-50/50' : ''
                    }`}>
                      <td className="px-5 py-3 font-mono text-xs text-navy-600">{esc.supplier_id}</td>
                      <td className="px-5 py-3 font-medium text-navy-800">{esc.supplier_name}</td>
                      <td className="px-5 py-3 text-gray-600">{esc.category}</td>
                      <td className="px-5 py-3"><RiskBadge level={esc.risk_level} /></td>
                      <td className="px-5 py-3 text-gray-600 max-w-xs">
                        <div className="space-y-0.5">
                          {issues.slice(0, 2).map((issue, i) => (
                            <p key={i} className="text-xs truncate" title={issue}>{issue}</p>
                          ))}
                          {issues.length > 2 && (
                            <p className="text-xs text-gray-400">+{issues.length - 2} more</p>
                          )}
                        </div>
                      </td>
                      <td className="px-5 py-3 text-right">
                        {esc.review_status === 'APPROVED' ? (
                          <span className="inline-flex items-center gap-1 text-green-600 font-medium text-xs">
                            <CheckCircle size={14} /> Approved
                          </span>
                        ) : esc.review_status === 'REJECTED' ? (
                          <span className="inline-flex items-center gap-1 text-red-600 font-medium text-xs">
                            <XCircle size={14} /> Rejected
                          </span>
                        ) : (
                          <div className="flex items-center justify-end gap-2">
                            <button
                              onClick={() => handleReview(esc.supplier_id, 'APPROVED')}
                              disabled={actionLoading === esc.supplier_id}
                              className="inline-flex items-center gap-1 px-3 py-1.5 text-xs font-medium rounded-lg bg-green-50 text-green-700 border border-green-200 hover:bg-green-100 transition-colors disabled:opacity-50"
                            >
                              {actionLoading === esc.supplier_id ? (
                                <Loader2 size={12} className="animate-spin" />
                              ) : (
                                <CheckCircle size={12} />
                              )}
                              Approve
                            </button>
                            <button
                              onClick={() => handleReview(esc.supplier_id, 'REJECTED')}
                              disabled={actionLoading === esc.supplier_id}
                              className="inline-flex items-center gap-1 px-3 py-1.5 text-xs font-medium rounded-lg bg-red-50 text-red-700 border border-red-200 hover:bg-red-100 transition-colors disabled:opacity-50"
                            >
                              {actionLoading === esc.supplier_id ? (
                                <Loader2 size={12} className="animate-spin" />
                              ) : (
                                <XCircle size={12} />
                              )}
                              Reject
                            </button>
                          </div>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {filtered.length > 50 && (
              <div className="px-5 py-3 bg-gray-50 text-center text-sm text-gray-500">
                Showing 50 of {filtered.length} escalations. Use filters or search to narrow results.
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
