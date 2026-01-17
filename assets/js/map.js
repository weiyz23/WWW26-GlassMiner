document.addEventListener("DOMContentLoaded", () => {
  const map = L.map('map').setView([20, 0], 2);

  // 添加底图
  L.tileLayer('https://{s}.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png', {}).addTo(map);

  // const servers = {{ site.data.lookingglass | jsonify }};
  const groupMap = new Map();

  // 按 (country, city) 分组，过滤掉均为"" 的服务器
  servers.forEach(s => {
    const key = `${s.country}||${s.city}`;
    if (s.country === "" && s.city === "") {
      return; // 跳过均为 "" 的服务器
    }
    if (!groupMap.has(key)) {
      groupMap.set(key, []);
    }
    groupMap.get(key).push(s);
  });

  // 绑定 marker 并在点击时更新表格
  groupMap.forEach((group, key) => {
    const [country, city] = key.split("||");
    const lat = group[0].lat;
    const lon = group[0].lon;
    const vpCount = group.length; // 计算城市中 VP 数量

    const marker = L.marker([lat, lon], { icon: customIcon })
      .addTo(map)
      .bindPopup(`
        <strong>${city}, ${country}</strong><br>
        <strong>VP Count: ${vpCount}</strong>
      `);

    marker.on('click', () => {
      const tbody = document.querySelector('#lgTable tbody');
      // 更新计数器
      const countDisplay = document.querySelector('#vpCountDisplay');
      // 更新 VP 数量显示，根据length确定使用 There are 还是 There is
      countDisplay.innerHTML = `<strong>There ${vpCount > 1 ? 'are' : 'is'} ${vpCount} VP${vpCount > 1 ? 's' : ''} in ${city}, ${country}.</strong>`;

      tbody.innerHTML = group.map(server => {
        return `
          <tr data-commands="${server.command.join(',')}">
            <td><strong><a href="${server.url}" target="_blank">${server.url}</a></strong></td>
            <td>${server.ip_addr}</td>
            <td>${server.country}</td>
            <td>${server.asn}</td>
            <td>
              ${server.command.map(cmd => `<span class="command-tag">${cmd}</span>`).join('')}
            </td>
            <td>
              <input type="checkbox" ${server.automatable ? 'checked' : ''} disabled>
            </td>
          </tr>
        `;
      }).join('');
    });
  });
});
