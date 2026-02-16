import { c as t, j as o, A as s } from "./index-DxrWcVFD.js";
import { O as u } from "./index-DxrWcVFD.js";
function m(r, e = {}) {
  const n = t.createRoot(r);
  return n.render(/* @__PURE__ */ o.jsx(s, { baseUrl: e.baseUrl ?? "", basename: e.basename ?? "/ojs/admin" })), () => n.unmount();
}
export {
  u as OJSAdminClient,
  m as mountOJSAdmin
};
