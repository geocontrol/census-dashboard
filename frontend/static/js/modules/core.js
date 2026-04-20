const API = '/api';

function getFeatureCode(feature) {
  return feature.properties.LSOA21CD
      || feature.properties.DZ22CD
      || feature.properties.DZ2021CD
      || '';
}

function getFeatureName(feature) {
  return feature.properties.LSOA21NM
      || feature.properties.DZ22NM
      || feature.properties.DZ2021NM
      || getFeatureCode(feature);
}

function isScotlandFeature(feature) {
  return feature.properties.nation === 'SC' || !!feature.properties.DZ22CD;
}

function isNorthernIrelandFeature(feature) {
  return feature.properties.nation === 'NI' || !!feature.properties.DZ2021CD;
}

const COLOUR_SCHEMES = {
  YlOrRd:['#ffffb2','#fed976','#feb24c','#fd8d3c','#fc4e2a','#e31a1c','#b10026'],
  PuBu:['#f1eef6','#d0d1e6','#a6bddb','#74a9cf','#3690c0','#0570b0','#034e7b'],
  BuGn:['#edf8fb','#ccece6','#99d8c9','#66c2a4','#41ae76','#238b45','#005824'],
  GnBu:['#f0f9e8','#ccebc5','#a8ddb5','#7bccc4','#4eb3d3','#2b8cbe','#08589e'],
  OrRd:['#fef0d9','#fdd49e','#fdbb84','#fc8d59','#ef6548','#d7301f','#990000'],
  YlGn:['#ffffe5','#f7fcb9','#d9f0a3','#addd8e','#78c679','#31a354','#006837'],
  Reds:['#fee5d9','#fcbba1','#fc9272','#fb6a4a','#ef3b2c','#cb181d','#99000d'],
  Greens:['#f7fcf5','#e5f5e0','#c7e9c0','#a1d99b','#74c476','#41ab5d','#005a32'],
  PuRd:['#f1eef6','#d4b9da','#c994c7','#df65b0','#e7298a','#ce1256','#91003f'],
  Blues:['#eff3ff','#c6dbef','#9ecae1','#6baed6','#4292c6','#2171b5','#084594'],
  Purples:['#f2f0f7','#dadaeb','#bcbddc','#9e9ac8','#807dba','#6a51a3','#4a1486'],
  YlOrBr:['#ffffe5','#fff7bc','#fee391','#fec44f','#fe9929','#ec7014','#cc4c02'],
  Greys:['#f7f7f7','#d9d9d9','#bdbdbd','#969696','#737373','#525252','#252525'],
  BuPu:['#edf8fb','#bfd3e6','#9ebcda','#8c96c6','#8c6bb1','#88419d','#6e016b'],
  RdPu:['#feebe2','#fcc5c0','#fa9fb5','#f768a1','#dd3497','#ae017e','#7a0177'],
  YlGnBu:['#ffffd9','#edf8b1','#c7e9b4','#7fcdbb','#41b6c4','#1d91c0','#225ea8'],
};

const state = {
  map: null, geojsonLayer: null,
  currentDataset: 'population_density', currentLAD: '',
  datasets: {}, currentValues: {}, currentStats: {},
  currentColorScheme: 'YlOrRd', quantileBreaks: [],
  selectedLSOA: null, geojsonData: null, boundariesReady: false,
  selectMode: false,
  selectedLSOAs: new Set(),
  dissolvedLayer: null,
  dissolveResult: null,
  adjacency: null,
  electionOverlay: null,
  electionData: null,
  electionMode: null,
  electionYear: null,
  electionMetric: 'winner',
};

let hoverPopup = null;
