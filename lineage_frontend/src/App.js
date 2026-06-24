import React, { useState, useCallback, useEffect } from 'react';
import ReactFlow, {
  Background, MiniMap,
  useNodesState, useEdgesState,
  MarkerType, useReactFlow, ReactFlowProvider,
  Handle, Position,
} from 'reactflow';
import 'reactflow/dist/style.css';
import { Search, X, GitBranch, ZoomIn, ZoomOut, Maximize2, RotateCcw } from 'lucide-react';

const API = process.env.REACT_APP_API_URL || 'http://localhost:8085';

const LAYER_COLORS = {
  bronze:  { bg: '#FEF3C7', border: '#D97706', text: '#92400E', dot: '#F59E0B' },
  silver:  { bg: '#F0F9FF', border: '#0284C7', text: '#0C4A6E', dot: '#38BDF8' },
  gold:    { bg: '#F0FDF4', border: '#16A34A', text: '#14532D', dot: '#4ADE80' },
  unknown: { bg: '#F5F3FF', border: '#7C3AED', text: '#4C1D95', dot: '#A78BFA' },
};

function LineageNode({ data, selected }) {
  const c = LAYER_COLORS[data.layer] || LAYER_COLORS.unknown;
  return (
    <div style={{
      background: selected ? c.border : c.bg,
      border: `2px solid ${c.border}`,
      borderRadius: 10, padding: '10px 14px',
      minWidth: 155, maxWidth: 190, cursor: 'pointer',
      boxShadow: selected ? `0 0 0 3px ${c.border}44` : '0 2px 8px rgba(0,0,0,0.08)',
      transition: 'all 0.2s', fontFamily: 'monospace',
      position: 'relative',
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
        <div style={{ width: 8, height: 8, borderRadius: '50%', background: c.dot, flexShrink: 0 }} />
        <span style={{ fontSize: 9, fontWeight: 700, letterSpacing: 1, color: selected ? 'white' : c.text, textTransform: 'uppercase' }}>
          {data.layer}
        </span>
        {data.node_type === 'hive_table' && (
          <span style={{ fontSize: 8, background: selected ? 'rgba(255,255,255,0.25)' : c.border + '22', color: selected ? 'white' : c.border, borderRadius: 3, padding: '1px 4px', marginLeft: 'auto' }}>
            Hive
          </span>
        )}
      </div>
      <div style={{ fontSize: 12, fontWeight: 600, color: selected ? 'white' : '#1a1a2e', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
        {data.table_name}
      </div>
      {data.row_count > 0 && (
        <div style={{ fontSize: 10, color: selected ? 'rgba(255,255,255,0.75)' : '#6b7280', marginTop: 3 }}>
          {data.row_count.toLocaleString()} filas
        </div>
      )}
      <Handle type="target" position={Position.Left} style={{ background: c.border, width: 8, height: 8, border: '2px solid white' }} />
      <Handle type="source" position={Position.Right} style={{ background: c.border, width: 8, height: 8, border: '2px solid white' }} />
    </div>
  );
}

const nodeTypes = { lineageNode: LineageNode };

function sanitizeId(nodeId) {
  return nodeId.replace(/[^a-zA-Z0-9_-]/g, '_');
}

function deduplicateNodes(rawNodes) {
  function tableName(nodeId) {
    return nodeId.replace('hive://', '').replace('s3a://', '')
      .split('/').pop().split('.').pop();
  }
  const byKey = {};
  rawNodes.forEach(n => {
    const key = `${n.layer}|${tableName(n.node_id)}`;
    if (!byKey[key]) { byKey[key] = n; }
    else { if (n.node_type === 'hive_table') byKey[key] = n; }
  });
  const canonical = {};
  rawNodes.forEach(n => {
    const key = `${n.layer}|${tableName(n.node_id)}`;
    canonical[n.node_id] = byKey[key].node_id;
  });
  return { nodes: Object.values(byKey), canonical };
}

function buildGridLayout(rawNodes, rawEdges) {
  const NODE_W = 200, NODE_H = 85, GAP_X = 20, GAP_Y = 20, GROUP_GAP = 120;
  const { nodes: dedupNodes, canonical } = deduplicateNodes(rawNodes);
  const layerOrder = ['bronze', 'silver', 'gold', 'unknown'];
  const byLayer = {};
  dedupNodes.forEach(n => {
    const l = n.layer || 'unknown';
    if (!byLayer[l]) byLayer[l] = [];
    byLayer[l].push(n);
  });
  function gridCols(count) {
    if (count <= 1) return 1;
    if (count <= 4) return 2;
    if (count <= 9) return 3;
    return 4;
  }
  const idMap = {};
  dedupNodes.forEach(n => { idMap[n.node_id] = sanitizeId(n.node_id); });
  const rfNodes = [];
  let groupX = 0;
  layerOrder.forEach(layer => {
    const layerNodes = byLayer[layer] || [];
    if (layerNodes.length === 0) return;
    const cols = gridCols(layerNodes.length);
    layerNodes.forEach((n, idx) => {
      const col = idx % cols;
      const row = Math.floor(idx / cols);
      rfNodes.push({
        id: idMap[n.node_id],
        type: 'lineageNode',
        position: { x: groupX + col * (NODE_W + GAP_X), y: row * (NODE_H + GAP_Y) },
        data: { ...n },
      });
    });
    groupX += cols * (NODE_W + GAP_X) + GROUP_GAP;
  });
  const sanitizedIds = new Set(rfNodes.map(n => n.id));
  const tableIndex = {};
  rfNodes.forEach(n => {
    const tname = n.data.table_name;
    const layer = n.data.layer;
    tableIndex[`${layer}|${tname}`] = n.id;
    if (!tableIndex[tname]) tableIndex[tname] = n.id;
  });
  function resolveToRFId(nodeId) {
    const can = canonical[nodeId];
    if (can) { const s = sanitizeId(can); if (sanitizedIds.has(s)) return s; }
    const direct = sanitizeId(nodeId);
    if (sanitizedIds.has(direct)) return direct;
    const parts = nodeId.replace('hive://', '').replace('s3a://', '').split('/');
    const tname = parts[parts.length - 1].split('.').pop();
    if (tableIndex[tname]) return tableIndex[tname];
    return null;
  }

  const seenEdges = new Set();
  const rfEdges = [];
  let edgeIdx = 0;

  rawEdges.forEach((e) => {
    const srcSanitized = resolveToRFId(e.source_id);
    const tgtSanitized = resolveToRFId(e.target_id);
    if (!srcSanitized || !tgtSanitized) return;
    if (srcSanitized === tgtSanitized) return;

    if (e.dag_id) {
      const dagKey = `dag|${srcSanitized}|${tgtSanitized}|${e.dag_id}`;
      if (!seenEdges.has(dagKey)) {
        seenEdges.add(dagKey);
        const dagLabel = e.dag_id
          .replace('ingesta_adventureworks', 'Ingesta AW')
          .replace('aw_bronze_to_silver', 'B→S: AW')
          .replace('aw_silver_to_gold', 'S→G: AW')
          .replace('bronze_to_silver_', 'B→S: ')
          .replace('silver_to_gold_', 'S→G: ');
        rfEdges.push({
          id: `dag_${edgeIdx++}`,
          source: srcSanitized,
          target: tgtSanitized,
          type: 'smoothstep',
          label: dagLabel,
          labelStyle: { fontSize: 8, fill: '#0284c7', fontWeight: 600 },
          labelBgStyle: { fill: 'white', fillOpacity: 0.9 },
          labelBgPadding: [3, 4],
          style: { stroke: '#93c5fd', strokeWidth: 2, strokeDasharray: 'none' },
          markerEnd: { type: MarkerType.ArrowClosed, color: '#93c5fd', width: 16, height: 16 },
          zIndex: 10,
          data: { ...e, edgeType: 'dag' },
        });
      }
    }

    if (e.script) {
      const scriptKey = `script|${srcSanitized}|${tgtSanitized}|${e.script}`;
      if (!seenEdges.has(scriptKey)) {
        seenEdges.add(scriptKey);
        rfEdges.push({
          id: `script_${edgeIdx++}`,
          source: srcSanitized,
          target: tgtSanitized,
          type: 'smoothstep',
          label: e.script.replace('.py', ''),
          labelStyle: { fontSize: 8, fill: '#d97706', fontWeight: 600 },
          labelBgStyle: { fill: 'white', fillOpacity: 0.9 },
          labelBgPadding: [3, 4],
          style: { stroke: '#fcd34d', strokeWidth: 2, strokeDasharray: '6,4' },
          markerEnd: { type: MarkerType.ArrowClosed, color: '#fcd34d', width: 16, height: 16 },
          zIndex: 9,
          data: { ...e, edgeType: 'script' },
        });
      }
    }
  });

  return { rfNodes, rfEdges };
}

function DetailPanel({ node, onClose }) {
  const [detail, setDetail] = useState(null);
  const [loading, setLoading] = useState(false);
  const c = LAYER_COLORS[node?.layer] || LAYER_COLORS.unknown;
  useEffect(() => {
    if (!node) return;
    setDetail(null); setLoading(true);
    fetch(`${API}/nodes/${encodeURIComponent(node.node_id)}`)
      .then(r => r.json()).then(d => { setDetail(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, [node?.node_id]);
  if (!node) return null;
  return (
    <div style={{ width: 320, background: '#fff', borderLeft: '1px solid #e5e7eb', display: 'flex', flexDirection: 'column', overflow: 'hidden', fontFamily: 'system-ui,sans-serif' }}>
      <div style={{ padding: '16px 20px', borderBottom: '1px solid #e5e7eb', background: c.bg }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
          <div>
            <div style={{ fontSize: 10, fontWeight: 700, letterSpacing: 1, color: c.text, textTransform: 'uppercase', marginBottom: 4 }}>Dataset · {node.layer?.toUpperCase()}</div>
            <div style={{ fontSize: 16, fontWeight: 700, color: '#111827' }}>{node.table_name}</div>
          </div>
          <button onClick={onClose} style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#6b7280', padding: 4 }}><X size={16} /></button>
        </div>
      </div>
      <div style={{ flex: 1, overflowY: 'auto', padding: '16px 20px' }}>
        {loading ? <div style={{ textAlign: 'center', padding: 40, color: '#9ca3af' }}>Cargando...</div> : detail ? (
          <>
            <Sec title="Resumen">
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 8 }}>
                <MiniCard label="Upstream" value={detail.upstream_count} />
                <MiniCard label="Downstream" value={detail.downstream_count} />
                <MiniCard label="Tipo" value={detail.node_type === 'hive_table' ? 'Tabla' : 'File'} />
              </div>
            </Sec>
            <Sec title="Ubicación">
              <KV label="Base de datos" value={detail.database || '—'} />
              <KV label="Formato" value={detail.format || '—'} />
              {detail.location && <KV label="Ruta" value={detail.location.length > 38 ? '…' + detail.location.slice(-35) : detail.location} mono />}
            </Sec>
            <Sec title="Estadísticas">
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
                <BigCard label="Filas" value={detail.row_count ? detail.row_count.toLocaleString() : '—'} />
                <BigCard label="Columnas" value={detail.column_count || '—'} />
              </div>
            </Sec>
            {detail.scanned_at && (
              <Sec title="Scanner">
                <KV label="Fuente" value="hive" />
                <KV label="Capturado" value={new Date(detail.scanned_at).toLocaleTimeString('es-BO', { hour: '2-digit', minute: '2-digit' })} />
              </Sec>
            )}
            {detail.columns?.length > 0 && (
              <Sec title={`Esquema (${detail.columns.length} columnas)`}>
                <div style={{ border: '1px solid #e5e7eb', borderRadius: 8, overflow: 'hidden' }}>
                  <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                    <thead>
                      <tr style={{ background: '#f9fafb' }}>
                        <th style={{ padding: '6px 10px', textAlign: 'left', color: '#6b7280', fontWeight: 600, borderBottom: '1px solid #e5e7eb' }}>Columna</th>
                        <th style={{ padding: '6px 10px', textAlign: 'left', color: '#6b7280', fontWeight: 600, borderBottom: '1px solid #e5e7eb' }}>Tipo</th>
                      </tr>
                    </thead>
                    <tbody>
                      {detail.columns.map((col, i) => (
                        <tr key={i} style={{ borderBottom: i < detail.columns.length - 1 ? '1px solid #f3f4f6' : 'none' }}>
                          <td style={{ padding: '5px 10px', color: '#111827', fontFamily: 'monospace' }}>{col.name}</td>
                          <td style={{ padding: '5px 10px' }}>
                            <span style={{ fontSize: 10, background: '#f0f9ff', color: '#0284c7', borderRadius: 3, padding: '1px 5px', fontFamily: 'monospace' }}>{col.type}</span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </Sec>
            )}
          </>
        ) : null}
      </div>
    </div>
  );
}

function EdgeDetailPanel({ edge, onClose }) {
  if (!edge) return null;
  const isDag = edge.edgeType === 'dag';
  return (
    <div style={{ width: 320, background: '#fff', borderLeft: '1px solid #e5e7eb', display: 'flex', flexDirection: 'column', overflow: 'hidden', fontFamily: 'system-ui,sans-serif' }}>
      <div style={{ padding: '16px 20px', borderBottom: '1px solid #e5e7eb', background: isDag ? '#F0F9FF' : '#FFFBEB' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
          <div>
            <div style={{ fontSize: 10, fontWeight: 700, letterSpacing: 1, color: isDag ? '#0C4A6E' : '#92400E', textTransform: 'uppercase', marginBottom: 4 }}>
              {isDag ? 'Trazabilidad DAG' : 'Trazabilidad Script'}
            </div>
            <div style={{ fontSize: 14, fontWeight: 700, color: '#111827' }}>
              {isDag ? (edge.dag_id || '—') : (edge.script || '—')}
            </div>
          </div>
          <button onClick={onClose} style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#6b7280', padding: 4 }}><X size={16} /></button>
        </div>
      </div>
      <div style={{ flex: 1, overflowY: 'auto', padding: '16px 20px' }}>
        <Sec title="Conexión">
          <KV label="Origen" value={edge.source_id?.replace('hive://', '') || '—'} mono />
          <KV label="Destino" value={edge.target_id?.replace('hive://', '') || '—'} mono />
        </Sec>
        <Sec title="Orquestación">
          <KV label="DAG Airflow" value={edge.dag_id || '—'} />
          <KV label="Script Python" value={edge.script || '—'} mono />
          <KV label="Capa" value={edge.layer || '—'} />
        </Sec>
        {edge.transformations && (
          <Sec title="Transformaciones aplicadas">
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
              {edge.transformations.split(',').map((t, i) => (
                <span key={i} style={{ fontSize: 11, background: isDag ? '#E0F2FE' : '#FEF3C7', color: isDag ? '#0369A1' : '#92400E', borderRadius: 4, padding: '3px 8px', fontFamily: 'monospace', fontWeight: 600 }}>
                  {t.trim()}
                </span>
              ))}
            </div>
          </Sec>
        )}
      </div>
    </div>
  );
}

function Sec({ title, children }) {
  return (
    <div style={{ marginBottom: 20 }}>
      <div style={{ fontSize: 11, fontWeight: 700, color: '#6b7280', textTransform: 'uppercase', letterSpacing: 0.8, marginBottom: 8 }}>{title}</div>
      {children}
    </div>
  );
}
function KV({ label, value, mono }) {
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6, gap: 8 }}>
      <span style={{ fontSize: 12, color: '#6b7280', flexShrink: 0 }}>{label}</span>
      <span style={{ fontSize: 12, color: '#111827', textAlign: 'right', wordBreak: 'break-all', fontFamily: mono ? 'monospace' : 'inherit' }}>{value}</span>
    </div>
  );
}
function MiniCard({ label, value }) {
  return (
    <div style={{ background: '#f9fafb', borderRadius: 8, padding: '10px 8px', textAlign: 'center', border: '1px solid #e5e7eb' }}>
      <div style={{ fontSize: 16, fontWeight: 700, color: '#111827' }}>{value}</div>
      <div style={{ fontSize: 10, color: '#9ca3af', marginTop: 2 }}>{label}</div>
    </div>
  );
}
function BigCard({ label, value }) {
  return (
    <div style={{ background: '#f9fafb', borderRadius: 8, padding: '10px 12px', border: '1px solid #e5e7eb' }}>
      <div style={{ fontSize: 11, color: '#9ca3af', marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: 18, fontWeight: 700, color: '#111827' }}>{value}</div>
    </div>
  );
}

function InnerApp() {
  const { zoomIn, zoomOut, fitView } = useReactFlow();
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [selectedNode, setSelectedNode] = useState(null);
  const [selectedEdge, setSelectedEdge] = useState(null);
  const [direction, setDirection] = useState('downstream');
  const [depth, setDepth] = useState(3);
  const [searchQ, setSearchQ] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [showSearch, setShowSearch] = useState(false);
  const [loading, setLoading] = useState(false);
  const [isLineageMode, setIsLineageMode] = useState(false);

  useEffect(() => { loadFull(); }, []);

  function loadFull() {
    setLoading(true);
    setIsLineageMode(false);
    setSelectedNode(null);
    setSelectedEdge(null);
    Promise.all([
      fetch(`${API}/nodes`).then(r => r.json()),
      fetch(`${API}/edges`).then(r => r.json()),
    ]).then(([rawNodes, rawEdges]) => {
      const { rfNodes, rfEdges } = buildGridLayout(rawNodes, rawEdges);
      setNodes(rfNodes);
      setEdges(rfEdges);
      setLoading(false);
      setTimeout(() => fitView({ padding: 0.15 }), 150);
    }).catch(() => setLoading(false));
  }

  function loadLineage(nodeId) {
    setLoading(true);
    setIsLineageMode(true);
    setSelectedEdge(null);
    fetch(`${API}/lineage/${direction}/${encodeURIComponent(nodeId)}?depth=${depth}`)
      .then(r => r.json())
      .then(data => {
        const { rfNodes, rfEdges } = buildGridLayout(data.nodes || [], data.edges || []);
        setNodes(rfNodes);
        setEdges(rfEdges);
        setLoading(false);
        setTimeout(() => fitView({ padding: 0.25 }), 150);
      })
      .catch(() => setLoading(false));
  }

  useEffect(() => {
    if (!searchQ || searchQ.length < 2) { setSearchResults([]); return; }
    const t = setTimeout(() => {
      fetch(`${API}/search?q=${encodeURIComponent(searchQ)}`)
        .then(r => r.json()).then(setSearchResults).catch(() => {});
    }, 300);
    return () => clearTimeout(t);
  }, [searchQ]);

  function handleSearchSelect(result) {
    setSearchQ(''); setSearchResults([]); setShowSearch(false);
    setSelectedNode(result);
    setSelectedEdge(null);
    loadLineage(result.node_id);
  }

  function handleNodeClick(_, node) {
    setSelectedEdge(null);
    setSelectedNode(node.data);
  }

  function handleEdgeClick(_, edge) {
    setSelectedNode(null);
    if (edge.data) {
      setSelectedEdge(edge.data);
    }
  }

  return (
    <div style={{ height: '100vh', display: 'flex', flexDirection: 'column', fontFamily: 'system-ui,sans-serif', background: '#f8fafc', overflow: 'hidden' }}>
      <div style={{ height: 56, background: '#fff', borderBottom: '1px solid #e5e7eb', display: 'flex', alignItems: 'center', padding: '0 20px', gap: 16, flexShrink: 0, boxShadow: '0 1px 3px rgba(0,0,0,0.05)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <div style={{ width: 28, height: 28, background: 'linear-gradient(135deg, #0284c7, #16a34a)', borderRadius: 8, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <GitBranch size={16} color="white" />
          </div>
          <span style={{ fontWeight: 700, fontSize: 15, color: '#111827' }}>Lineage Explorer</span>
        </div>

        <div style={{ flex: 1, maxWidth: 420, position: 'relative', zIndex: 200 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, background: '#f3f4f6', borderRadius: 8, padding: '7px 12px', border: `1px solid ${showSearch ? '#0284c7' : 'transparent'}` }}>
            <Search size={14} color="#9ca3af" />
            <input value={searchQ}
              onChange={e => { setSearchQ(e.target.value); setShowSearch(true); }}
              onFocus={() => setShowSearch(true)}
              placeholder="Buscar tabla o dataset... (activa linaje)"
              style={{ background: 'none', border: 'none', outline: 'none', fontSize: 13, color: '#111827', flex: 1 }}
            />
            {searchQ && <button onClick={() => { setSearchQ(''); setSearchResults([]); }} style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#9ca3af' }}><X size={12} /></button>}
          </div>
          {showSearch && searchResults.length > 0 && (
            <div style={{ position: 'absolute', top: '100%', left: 0, right: 0, background: 'white', borderRadius: 8, marginTop: 4, boxShadow: '0 8px 24px rgba(0,0,0,0.12)', zIndex: 300, border: '1px solid #e5e7eb', overflow: 'hidden' }}>
              {searchResults.map((r, i) => {
                const c = LAYER_COLORS[r.layer] || LAYER_COLORS.unknown;
                return (
                  <div key={i} onClick={() => handleSearchSelect(r)}
                    style={{ padding: '10px 14px', cursor: 'pointer', borderBottom: i < searchResults.length - 1 ? '1px solid #f3f4f6' : 'none', display: 'flex', alignItems: 'center', gap: 10 }}
                    onMouseEnter={e => e.currentTarget.style.background = '#f9fafb'}
                    onMouseLeave={e => e.currentTarget.style.background = 'white'}
                  >
                    <div style={{ width: 8, height: 8, borderRadius: '50%', background: c.dot }} />
                    <div>
                      <div style={{ fontSize: 13, fontWeight: 600, color: '#111827' }}>{r.table_name}</div>
                      <div style={{ fontSize: 11, color: '#9ca3af' }}>{r.layer} · {r.format}</div>
                    </div>
                    <div style={{ marginLeft: 'auto', fontSize: 10, color: '#0284c7', fontWeight: 600 }}>Ver linaje →</div>
                  </div>
                );
              })}
            </div>
          )}
        </div>

        <div style={{ display: 'flex', gap: 4, marginLeft: 'auto' }}>
          {['upstream', 'downstream'].map(d => (
            <button key={d} onClick={() => {
              setDirection(d);
              if (selectedNode) {
                setIsLineageMode(true);
                setSelectedEdge(null);
                fetch(`${API}/lineage/${d}/${encodeURIComponent(selectedNode.node_id)}?depth=${depth}`)
                  .then(r => r.json())
                  .then(data => {
                    const { rfNodes, rfEdges } = buildGridLayout(data.nodes || [], data.edges || []);
                    setNodes(rfNodes);
                    setEdges(rfEdges);
                    setTimeout(() => fitView({ padding: 0.25 }), 150);
                  }).catch(() => {});
              }
            }}
              style={{ padding: '6px 14px', borderRadius: 6, fontSize: 12, fontWeight: 600, cursor: 'pointer', border: selectedNode ? '2px solid ' + (d === 'upstream' ? '#374151' : '#0284c7') : 'none', background: direction === d ? (d === 'upstream' ? '#374151' : '#0284c7') : '#f3f4f6', color: direction === d ? 'white' : '#6b7280', transition: 'all 0.2s' }}>
              {d === 'upstream' ? 'Backward (Upstream)' : 'Forward (Downstream)'}
            </button>
          ))}
        </div>
        {isLineageMode && <div style={{ fontSize: 11, background: '#fef3c7', color: '#92400e', padding: '4px 10px', borderRadius: 20, fontWeight: 600 }}>Modo Linaje</div>}
      </div>

      <div style={{ flex: 1, display: 'flex', overflow: 'hidden' }}>
        <div style={{ width: 48, background: '#fff', borderRight: '1px solid #e5e7eb', display: 'flex', flexDirection: 'column', alignItems: 'center', padding: '12px 0', gap: 4 }}>
          {[
            { icon: <ZoomIn size={18} />, tip: 'Zoom In', fn: () => zoomIn() },
            { icon: <ZoomOut size={18} />, tip: 'Zoom Out', fn: () => zoomOut() },
            { icon: <Maximize2 size={18} />, tip: 'Ajustar vista', fn: () => fitView({ padding: 0.15 }) },
            { icon: <RotateCcw size={18} />, tip: 'Grafo completo', fn: loadFull },
          ].map((item, i) => (
            <button key={i} onClick={item.fn} title={item.tip}
              style={{ width: 34, height: 34, borderRadius: 8, border: 'none', background: 'none', cursor: 'pointer', color: '#9ca3af', display: 'flex', alignItems: 'center', justifyContent: 'center' }}
              onMouseEnter={e => { e.currentTarget.style.background = '#f3f4f6'; e.currentTarget.style.color = '#374151'; }}
              onMouseLeave={e => { e.currentTarget.style.background = 'none'; e.currentTarget.style.color = '#9ca3af'; }}>
              {item.icon}
            </button>
          ))}
        </div>

        <div style={{ flex: 1, position: 'relative' }}>
          <div style={{ position: 'absolute', top: 12, left: 12, zIndex: 10, display: 'flex', alignItems: 'center', gap: 8, background: 'white', borderRadius: 8, padding: '6px 12px', boxShadow: '0 2px 8px rgba(0,0,0,0.08)', border: '1px solid #e5e7eb' }}>
            <span style={{ fontSize: 12, color: '#6b7280', fontWeight: 600 }}>Profundidad:</span>
            {[1, 2, 3, 'N'].map(d => (
              <button key={d}
                onClick={() => { const v = d === 'N' ? 10 : d; setDepth(v); if (isLineageMode && selectedNode) loadLineage(selectedNode.node_id); }}
                style={{ width: 28, height: 28, borderRadius: 6, border: 'none', cursor: 'pointer', fontSize: 12, fontWeight: 600, background: depth === (d === 'N' ? 10 : d) ? '#0284c7' : '#f3f4f6', color: depth === (d === 'N' ? 10 : d) ? 'white' : '#374151' }}>
                {d}
              </button>
            ))}
          </div>

          <div style={{ position: 'absolute', bottom: 48, left: 12, zIndex: 10, background: 'white', borderRadius: 8, padding: '10px 14px', boxShadow: '0 2px 8px rgba(0,0,0,0.08)', border: '1px solid #e5e7eb', display: 'flex', gap: 14, alignItems: 'center' }}>
            {['bronze', 'silver', 'gold'].map(layer => {
              const c = LAYER_COLORS[layer];
              return (
                <div key={layer} style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
                  <div style={{ width: 12, height: 12, borderRadius: 3, background: c.bg, border: `2px solid ${c.border}` }} />
                  <span style={{ fontSize: 11, fontWeight: 600, color: '#374151', textTransform: 'capitalize' }}>{layer}</span>
                </div>
              );
            })}
            <div style={{ width: 1, height: 16, background: '#e5e7eb' }} />
            <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
              <svg width="24" height="8"><line x1="0" y1="4" x2="24" y2="4" stroke="#93c5fd" strokeWidth="2" /></svg>
              <span style={{ fontSize: 11, color: '#6b7280', fontWeight: 600 }}>DAG Airflow</span>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
              <svg width="24" height="8"><line x1="0" y1="4" x2="24" y2="4" stroke="#fcd34d" strokeWidth="2" strokeDasharray="5,3" /></svg>
              <span style={{ fontSize: 11, color: '#6b7280', fontWeight: 600 }}>Script Python</span>
            </div>
          </div>

          {loading && (
            <div style={{ position: 'absolute', top: '50%', left: '50%', transform: 'translate(-50%,-50%)', zIndex: 20, background: 'rgba(255,255,255,0.95)', borderRadius: 12, padding: '16px 24px', boxShadow: '0 4px 20px rgba(0,0,0,0.1)', fontSize: 13, color: '#6b7280' }}>
              Cargando...
            </div>
          )}

          <ReactFlow
            nodes={nodes} edges={edges}
            onNodesChange={onNodesChange} onEdgesChange={onEdgesChange}
            nodeTypes={nodeTypes}
            onNodeClick={handleNodeClick}
            onEdgeClick={handleEdgeClick}
            onPaneClick={() => { setShowSearch(false); setSelectedEdge(null); }}
            fitView fitViewOptions={{ padding: 0.15 }}
            minZoom={0.15} maxZoom={2}
          >
            <Background color="#e5e7eb" gap={20} size={1} />
            <MiniMap nodeColor={n => LAYER_COLORS[n.data?.layer]?.dot || '#a78bfa'} maskColor="rgba(240,249,255,0.7)" style={{ bottom: 48 }} />
          </ReactFlow>
        </div>

        {selectedNode && <DetailPanel node={selectedNode} onClose={() => setSelectedNode(null)} />}
        {selectedEdge && <EdgeDetailPanel edge={selectedEdge} onClose={() => setSelectedEdge(null)} />}
      </div>

      <div style={{ height: 36, background: '#fff', borderTop: '1px solid #e5e7eb', display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '0 20px', flexShrink: 0 }}>
        <span style={{ fontSize: 11, color: '#9ca3af' }}>
          <strong style={{ color: '#374151' }}>Clic en nodo</strong> → ver info &nbsp;·&nbsp;
          <strong style={{ color: '#374151' }}>Clic en línea</strong> → ver transformación &nbsp;·&nbsp;
          <strong style={{ color: '#374151' }}>Buscar</strong> → activa linaje &nbsp;·&nbsp;
          <strong style={{ color: '#374151' }}>↺</strong> → grafo completo
        </span>
        <span style={{ fontSize: 11, color: '#9ca3af' }}>Tesis UMSA 2026 · Lago de Datos Federado</span>
      </div>

      {showSearch && searchResults.length > 0 && (
        <div style={{ position: 'fixed', inset: 0, zIndex: 100 }} onClick={() => setShowSearch(false)} />
      )}
    </div>
  );
}

export default function App() {
  return <ReactFlowProvider><InnerApp /></ReactFlowProvider>;
}