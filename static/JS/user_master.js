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
      filterItem.innerHTML = `<span class="filter-key">${displayKey}:</span> <span class="filter-value">${displayValue}</span>`;
      filtersList.appendChild(filterItem);
    }
  });
  filtersStrip.style.display = hasFilters ? "block" : "none";
  return hasFilters;
}

// Smooth scroll to Page 2 (snap section)
function scrollToPage2() {
  const page2 = document.getElementById("page2");
  if (page2) {
    page2.scrollIntoView({ behavior: "smooth", block: "start" });
  }
}

// DOM ready
document.addEventListener("DOMContentLoaded", function () {
  const hasQueryParams = window.location.search.includes("=");
  const hasFilters = updateAppliedFilters();

  // Store form submission flag
  document.getElementById("filterForm").addEventListener("submit", function () {
    sessionStorage.setItem("filterSubmitted", "true");
  });

  // After reload, jump directly to Page 2 without jerky double-scroll
  if (sessionStorage.getItem("filterSubmitted") === "true") {
    sessionStorage.removeItem("filterSubmitted");
    setTimeout(() => {
      updateAppliedFilters();
      scrollToPage2();
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

      // Smooth scroll to table if showing
      if (tableContainer.style.display === "block") {
        setTimeout(() => {
          const yOffset = -20; // Adjust scroll position
          const y =
            tableContainer.getBoundingClientRect().top +
            window.pageYOffset +
            yOffset;
          window.scrollTo({ top: y, behavior: "smooth" });
        }, 100);
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
  const page1 = document.getElementById("page1");
  const page2 = document.getElementById("page2");

  // Function to check if we're on page 2, 3, or 4
  function checkPagePosition() {
    const page1Rect = page1.getBoundingClientRect();
    const page2Rect = page2.getBoundingClientRect();

    // If page1 is not fully visible or page2 is starting to come into view
    if (
      page1Rect.bottom <= window.innerHeight / 2 ||
      page2Rect.top <= window.innerHeight / 2
    ) {
      filterStrip.style.display = "block";
    } else {
      filterStrip.style.display = "none";
    }
  }

  // Initial check
  checkPagePosition();

  // Listen for scroll events
  window.addEventListener("scroll", checkPagePosition);

  // Listen for resize events
  window.addEventListener("resize", checkPagePosition);

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
