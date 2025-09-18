// Initialize date pickers
flatpickr(".datepicker", {
  dateFormat: "d/m/Y",
  allowInput: true,
});
// Auto-submit on TNI status change
document.getElementById("tni_status").addEventListener("change", function () {
  this.form.submit();
});
// Show loading state on form submit
document.querySelector("form").addEventListener("submit", function () {
  document.querySelector(".table-container").innerHTML = `
        <div class="loading-state">
            <div class="spinner-border" role="status">
                <span class="visually-hidden">Loading...</span>
            </div>
            <p class="mt-3">Applying filters...</p>
        </div>
    `;
});
// Reset pagination on filter change
document.querySelectorAll("form select, form input").forEach((element) => {
  if (element.name !== "page") {
    element.addEventListener("change", function () {
      document.querySelector('input[name="page"]').value = 1;
    });
  }
});
// Applied filters UI update
// Applied filters UI update
function updateAppliedFilters() {
  const form = document.getElementById("filterForm");
  const filtersList = document.getElementById("appliedFiltersList");
  const filtersStrip = document.getElementById("applied-filters");
  const formData = new FormData(form);
  let hasFilters = false;
  filtersList.innerHTML = "";

  formData.forEach((value, key) => {
    if (value && key !== "page" && value !== "All" && value !== "") {
      hasFilters = true;
      const filterItem = document.createElement("span");
      filterItem.className = "applied-filter-item";
      let displayKey = key.replace(/_/g, " ");
      displayKey = displayKey.charAt(0).toUpperCase() + displayKey.slice(1);
      let displayValue = value.replace(/_/g, " ");

      // Special formatting for fiscal year
      if (key === "fiscal_year") {
        const year = parseInt(value);
        if (!isNaN(year)) {
          const nextYear = year + 1;
          const nextYearShort = nextYear.toString().slice(-2); // Get last 2 digits
          displayValue = `FY ${year}-${nextYearShort}`;
          displayKey = "Fiscal Year"; // Keep the key as "Fiscal Year"
        }
      }

      filterItem.innerHTML = `<span class="filter-key">${displayKey}:</span> <span class="filter-value">${displayValue}</span>`;
      filtersList.appendChild(filterItem);
    }
  });

  filtersStrip.style.display = hasFilters ? "block" : "none";
  return hasFilters;
}
// DOM ready
document.addEventListener("DOMContentLoaded", function () {
  const hasQueryParams = window.location.search.includes("=");
  const hasFilters = updateAppliedFilters();
  // Store form submission flag
  document.getElementById("filterForm").addEventListener("submit", function () {
    sessionStorage.setItem("filterSubmitted", "true");
  });
  // After reload, update filters but don't scroll
  if (sessionStorage.getItem("filterSubmitted") === "true") {
    sessionStorage.removeItem("filterSubmitted");
    setTimeout(() => {
      updateAppliedFilters();
    }, 150); // Short delay to ensure layout is ready
  }
});
// Display current date
function displayCurrentDate() {
  const currentDate = new Date();
  const options = { year: "numeric", month: "long", day: "numeric" };
  const formattedDate = currentDate.toLocaleDateString("en-US", options);
  const dateElement = document.getElementById("current-date");
  if (dateElement) {
    dateElement.textContent = "Data updated as of " + formattedDate + ".";
  }
}
displayCurrentDate();
// Update date at midnight
const now = new Date();
const midnight = new Date(
  now.getFullYear(),
  now.getMonth(),
  now.getDate() + 1,
  0,
  0,
  0
);
const timeUntilMidnight = midnight - now;
setTimeout(function () {
  displayCurrentDate();
  setInterval(displayCurrentDate, 86400000);
}, timeUntilMidnight);
// Enhanced table toggle functionality
document.addEventListener("DOMContentLoaded", function () {
  const toggleBtn = document.getElementById("toggleTableBtn");
  const tableContainer = document.getElementById("resultsTable");
  // Check if we have table data to toggle
  if (toggleBtn && tableContainer) {
    // Check if table was previously visible
    const isTableVisible = localStorage.getItem("tableVisible") === "true";
    // Set initial state
    if (isTableVisible) {
      tableContainer.style.display = "block";
      toggleBtn.innerHTML = '<i class="fas fa-eye me-2"></i>Hide Table Data';
      toggleBtn.classList.remove("pulse");
    } else {
      tableContainer.style.display = "none";
      toggleBtn.innerHTML =
        '<i class="fas fa-eye-slash me-2"></i>Show Table Data';
      // Add pulse animation only if there are records
      if (document.querySelector(".table-responsive")) {
        toggleBtn.classList.add("pulse");
      }
    }
    // Toggle button click event
    toggleBtn.addEventListener("click", function () {
      if (tableContainer.style.display === "none") {
        tableContainer.style.display = "block";
        toggleBtn.innerHTML = '<i class="fas fa-eye me-2"></i>Hide Table Data';
        toggleBtn.classList.remove("pulse");
        localStorage.setItem("tableVisible", "true");
        // Add a slight highlight effect to the table
        tableContainer.style.opacity = "0.9";
        setTimeout(() => {
          tableContainer.style.opacity = "1";
        }, 300);
      } else {
        tableContainer.style.display = "none";
        toggleBtn.innerHTML =
          '<i class="fas fa-eye-slash me-2"></i>Show Table Data';
        // Add pulse animation only if there are records
        if (document.querySelector(".table-responsive")) {
          toggleBtn.classList.add("pulse");
        }
        localStorage.setItem("tableVisible", "false");
      }
    });
  }
  // Auto-show table if it contains data and we're on page 3
  const urlParams = new URLSearchParams(window.location.search);
  if (urlParams.toString() && window.location.hash === "#page3") {
    setTimeout(() => {
      if (tableContainer && document.querySelector(".table-responsive")) {
        tableContainer.style.display = "block";
        if (toggleBtn) {
          toggleBtn.innerHTML =
            '<i class="fas fa-eye me-2"></i>Hide Table Data';
          toggleBtn.classList.remove("pulse");
        }
        localStorage.setItem("tableVisible", "true");
      }
    }, 500);
  }
  // Add keyboard shortcut (Alt+T) to toggle table
  document.addEventListener("keydown", function (e) {
    if (e.altKey && e.key.toLowerCase() === "t") {
      e.preventDefault();
      if (toggleBtn) {
        toggleBtn.click();
      }
    }
  });
});
document.addEventListener("DOMContentLoaded", function () {
  const filterStrip = document.getElementById("applied-filters");

  // Update the current date
  function updateDate() {
    const now = new Date();
    const options = { year: "numeric", month: "short", day: "numeric" };
    document.getElementById(
      "current-date"
    ).textContent = now.toLocaleDateString(undefined, options);
  }
  updateDate();
  // Update date every minute
  setInterval(updateDate, 60000);
});
document.addEventListener("DOMContentLoaded", function () {
  const filterToggleBtn = document.getElementById("filterToggleBtn");
  const filterSidePanel = document.getElementById("filterSidePanel");
  const filterControlArea = document.getElementById("filterControlArea");
  const filterLabelContainer = document.getElementById("filterLabelContainer"); // Add this line
  // Toggle filter panel when clicking the toggle button
  filterToggleBtn.addEventListener("click", function () {
    filterSidePanel.classList.toggle("active");
    filterControlArea.classList.toggle("panel-open");
  });
  // Toggle filter panel when clicking the "Choose Filter" label
  filterLabelContainer.addEventListener("click", function () {
    filterSidePanel.classList.toggle("active");
    filterControlArea.classList.toggle("panel-open");
  });
  // Close filter panel when clicking outside
  document.addEventListener("click", function (event) {
    if (
      !filterSidePanel.contains(event.target) &&
      !filterToggleBtn.contains(event.target) &&
      !filterControlArea.contains(event.target) &&
      filterSidePanel.classList.contains("active")
    ) {
      filterSidePanel.classList.remove("active");
      filterControlArea.classList.remove("panel-open");
    }
  });
  // Hide filter panel when filters are applied
  const filterForm = document.getElementById("filterForm");
  if (filterForm) {
    filterForm.addEventListener("submit", function () {
      filterSidePanel.classList.remove("active");
      filterControlArea.classList.remove("panel-open");
    });
  }
  // Also hide when reset button is clicked
  const resetButton = document.querySelector(
    "a[href=\"{{ url_for('view_bp.view_master_data') }}\"]"
  );
  if (resetButton) {
    resetButton.addEventListener("click", function () {
      filterSidePanel.classList.remove("active");
      filterControlArea.classList.remove("panel-open");
    });
  }
  // And when export button is clicked
  const exportButton = document.querySelector('a[href*="download_excel"]');
  if (exportButton) {
    exportButton.addEventListener("click", function () {
      filterSidePanel.classList.remove("active");
      filterControlArea.classList.remove("panel-open");
    });
  }
});